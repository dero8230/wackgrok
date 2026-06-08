package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/paskhal/wackgrok/internal/protocol"
)

// runControlListener accepts incoming client control connections.
func (s *Server) runControlListener(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.ControlAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.ControlAddr, err)
	}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return fmt.Errorf("accept: %w", err)
			}
		}
		go s.handleControlConn(ctx, conn)
	}
}

// handleControlConn manages the lifetime of a single client connection:
//  1. Reads and validates the authentication message.
//  2. Registers the tunnel under the assigned subdomain.
//  3. Enters the keep-alive loop (ping ↔ pong), cleaning up on disconnect.
func (s *Server) handleControlConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	// ---- Phase 1: authentication ----
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	msg, err := dec.Recv()
	if err != nil {
		log.Printf("[control] %s: read auth: %v", conn.RemoteAddr(), err)
		return
	}
	conn.SetDeadline(time.Time{})

	if msg.Type != protocol.MsgTypeAuth {
		enc.Send(protocol.MsgTypeAuthErr, protocol.AuthError{Reason: "expected auth message"})
		return
	}

	authReq, err := protocol.Unmarshal[protocol.AuthRequest](msg.Payload)
	if err != nil {
		enc.Send(protocol.MsgTypeAuthErr, protocol.AuthError{Reason: "malformed auth payload"})
		return
	}

	if !s.cfg.Tokens[authReq.Token] {
		enc.Send(protocol.MsgTypeAuthErr, protocol.AuthError{Reason: "invalid token"})
		log.Printf("[control] %s: rejected (bad token)", conn.RemoteAddr())
		return
	}

	// ---- Phase 2: subdomain assignment ----
	id := sanitizeSubdomain(authReq.Subdomain)

	tunnel := &Tunnel{ID: id, enc: enc}

	if !s.tunnels.Register(id, tunnel) {
		if authReq.Subdomain == "" {
			// Random collision (very unlikely) — just try again.
			for i := 0; i < 5; i++ {
				id = randomID(8)
				tunnel.ID = id
				if s.tunnels.Register(id, tunnel) {
					break
				}
			}
			if _, taken := s.tunnels.Get(id); taken && tunnel.ID != id {
				enc.Send(protocol.MsgTypeAuthErr, protocol.AuthError{Reason: "could not assign subdomain"})
				return
			}
		} else {
			// Requested subdomain is taken — fall back to random.
			id = randomID(8)
			tunnel.ID = id
			for !s.tunnels.Register(id, tunnel) {
				id = randomID(8)
				tunnel.ID = id
			}
			log.Printf("[control] %s: subdomain %q taken, assigned %q instead",
				conn.RemoteAddr(), sanitizeSubdomain(authReq.Subdomain), id)
		}
	}
	defer s.tunnels.Unregister(id)

	url := fmt.Sprintf("http://%s.%s", id, s.cfg.Domain)
	if err := enc.Send(protocol.MsgTypeAuthOK, protocol.AuthResponse{ID: id, URL: url}); err != nil {
		log.Printf("[control] %s: send auth_ok: %v", conn.RemoteAddr(), err)
		return
	}

	log.Printf("[control] tunnel registered: %s  remote=%s", url, conn.RemoteAddr())

	// ---- Phase 3: keep-alive loop ----
	readErr := make(chan error, 1)
	go func() {
		for {
			m, err := dec.Recv()
			if err != nil {
				readErr <- err
				return
			}
			switch m.Type {
			case protocol.MsgTypePong:
				// expected response to our ping — nothing to do
			default:
				log.Printf("[control] tunnel %s: unexpected message %q", id, m.Type)
			}
		}
	}()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-readErr:
			log.Printf("[control] tunnel %s: disconnected: %v", id, err)
			return
		case <-ticker.C:
			if err := tunnel.sendPing(); err != nil {
				log.Printf("[control] tunnel %s: ping failed: %v", id, err)
				return
			}
		}
	}
}
