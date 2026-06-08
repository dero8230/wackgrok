// Package server implements the wackgrok tunnel server.
//
// Architecture overview:
//
//	┌─────────────────────────────────────────────────────────────┐
//	│  VPS / Docker container                                     │
//	│                                                             │
//	│  :7070  control listener  ←── client TCP (JSON messages)   │
//	│  :7071  data    listener  ←── client TCP (per-request)     │
//	│  :80    HTTP    listener  ←── public internet              │
//	└─────────────────────────────────────────────────────────────┘
//
// When a public HTTP request arrives for <id>.wackgrok.paskhal.com:
//  1. The HTTP handler resolves the tunnel for <id>.
//  2. It sends a ProxyRequest (with a unique requestID) over the control channel.
//  3. The client opens a TCP data connection to :7071, sends the requestID.
//  4. The data handler pairs that connection with the waiting HTTP handler.
//  5. The HTTP handler writes the raw HTTP request to the data conn.
//  6. The client pipes data conn ↔ localhost:<localPort>.
//  7. The HTTP handler reads the HTTP response from the data conn and replies.
package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// Config holds all server configuration.
type Config struct {
	ControlAddr string          // TCP address for the control listener, e.g. ":7070"
	DataAddr    string          // TCP address for the data listener,    e.g. ":7071"
	HTTPAddr    string          // TCP address for the HTTP listener,     e.g. ":80"
	Domain      string          // Base domain, e.g. "wackgrok.paskhal.com"
	Tokens      map[string]bool // Set of valid authentication tokens
}

// Server is the central coordinator for all server-side components.
type Server struct {
	cfg     Config
	tunnels *TunnelRegistry

	pendingMu    sync.Mutex
	pendingConns map[string]chan net.Conn // requestID → data conn channel
}

// New creates a new Server with the given configuration.
func New(cfg Config) *Server {
	return &Server{
		cfg:          cfg,
		tunnels:      newTunnelRegistry(),
		pendingConns: make(map[string]chan net.Conn),
	}
}

// Run starts all listeners and blocks until ctx is cancelled or a fatal error
// occurs.
func (s *Server) Run(ctx context.Context) error {
	errc := make(chan error, 3)

	go func() {
		if err := s.runControlListener(ctx); err != nil {
			errc <- fmt.Errorf("control listener: %w", err)
		}
	}()

	go func() {
		if err := s.runDataListener(ctx); err != nil {
			errc <- fmt.Errorf("data listener: %w", err)
		}
	}()

	go func() {
		if err := s.runHTTPServer(ctx); err != nil {
			errc <- fmt.Errorf("http server: %w", err)
		}
	}()

	log.Printf("[wackgrok] server listening")
	log.Printf("[wackgrok]   control : %s", s.cfg.ControlAddr)
	log.Printf("[wackgrok]   data    : %s", s.cfg.DataAddr)
	log.Printf("[wackgrok]   http    : %s", s.cfg.HTTPAddr)
	log.Printf("[wackgrok]   domain  : %s", s.cfg.Domain)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return nil
	}
}

// ---- pending-connection helpers ----

// registerPending creates a buffered channel for requestID and stores it.
func (s *Server) registerPending(requestID string) chan net.Conn {
	ch := make(chan net.Conn, 1)
	s.pendingMu.Lock()
	s.pendingConns[requestID] = ch
	s.pendingMu.Unlock()
	return ch
}

// resolvePending looks up the channel for requestID, removes it from the map,
// and delivers conn to the waiting HTTP handler. Returns false if not found.
func (s *Server) resolvePending(requestID string, conn net.Conn) bool {
	s.pendingMu.Lock()
	ch, ok := s.pendingConns[requestID]
	if ok {
		delete(s.pendingConns, requestID)
	}
	s.pendingMu.Unlock()
	if ok {
		ch <- conn
	}
	return ok
}

// cleanPending removes the pending entry without delivering a connection
// (used on timeout cleanup).
func (s *Server) cleanPending(requestID string) {
	s.pendingMu.Lock()
	delete(s.pendingConns, requestID)
	s.pendingMu.Unlock()
}

// waitForConn blocks until the client sends a data connection for requestID,
// or until the context/timeout fires. The caller must NOT call cleanPending
// after a successful return — the channel is already removed by resolvePending.
func (s *Server) waitForConn(ctx context.Context, requestID string) (net.Conn, error) {
	ch := s.registerPending(requestID)

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	select {
	case conn := <-ch:
		return conn, nil
	case <-waitCtx.Done():
		s.cleanPending(requestID)
		if ctx.Err() != nil {
			return nil, fmt.Errorf("request cancelled")
		}
		return nil, fmt.Errorf("timed out waiting for client data connection")
	}
}
