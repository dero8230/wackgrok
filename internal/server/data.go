package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"time"
)

// runDataListener accepts incoming client data connections.
// Each data connection carries a single HTTP request/response exchange.
func (s *Server) runDataListener(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.DataAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.DataAddr, err)
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
		go s.handleDataConn(conn)
	}
}

// handleDataConn reads the request ID sent by the client as the first line,
// then hands the connection off to the waiting HTTP handler.
func (s *Server) handleDataConn(conn net.Conn) {
	// Give the client a short window to send the request ID.
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		log.Printf("[data] %s: failed to read request id: %v", conn.RemoteAddr(), scanner.Err())
		conn.Close()
		return
	}
	requestID := scanner.Text()
	conn.SetDeadline(time.Time{})

	if requestID == "" {
		log.Printf("[data] %s: empty request id", conn.RemoteAddr())
		conn.Close()
		return
	}

	if !s.resolvePending(requestID, conn) {
		log.Printf("[data] %s: unknown or expired request id %q", conn.RemoteAddr(), requestID)
		conn.Close()
	}
	// Ownership of conn is transferred to the HTTP handler via the channel.
}
