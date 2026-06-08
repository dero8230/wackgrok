// Package client implements the wackgrok tunnel client.
//
// Flow:
//  1. Dial the server control port and authenticate with a token.
//  2. Receive the assigned tunnel URL (e.g. http://abc123.wackgrok.paskhal.com).
//  3. Enter the event loop: on every ProxyRequest from the server, open a
//     data connection, identify it with the request ID, then bridge it with
//     the local service.
package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/paskhal/wackgrok/internal/protocol"
)

// Config holds all client configuration.
type Config struct {
	// ServerAddr is the host:port of the server control listener (e.g. "example.com:7070").
	ServerAddr string
	// DataAddr is the host:port of the server data listener (e.g. "example.com:7071").
	// If empty it is derived from ServerAddr by incrementing the port by 1.
	DataAddr string
	// Token is the authentication secret.
	Token string
	// LocalPort is the port of the local service to expose.
	LocalPort int
	// Subdomain is the requested tunnel name. Empty = server assigns a random one.
	Subdomain string
}

// Client manages the connection to the wackgrok server.
type Client struct {
	cfg Config
}

// New creates a new Client.
func New(cfg Config) *Client {
	if cfg.DataAddr == "" {
		cfg.DataAddr = deriveDataAddr(cfg.ServerAddr)
	}
	return &Client{cfg: cfg}
}

// Run connects to the server and reconnects automatically on disconnect.
// It blocks until ctx is cancelled.
func (c *Client) Run(ctx context.Context) error {
	for {
		err := c.connect(ctx)
		if ctx.Err() != nil {
			return nil // clean shutdown
		}
		log.Printf("[wackgrok] disconnected: %v — reconnecting in 5s…", err)
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return nil
		}
	}
}

// connect establishes one full session with the server.
func (c *Client) connect(ctx context.Context) error {
	conn, err := net.Dial("tcp", c.cfg.ServerAddr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.cfg.ServerAddr, err)
	}
	defer conn.Close()

	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	// ---- Authenticate ----
	if err := enc.Send(protocol.MsgTypeAuth, protocol.AuthRequest{
		Token:     c.cfg.Token,
		Subdomain: c.cfg.Subdomain,
	}); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	msg, err := dec.Recv()
	if err != nil {
		return fmt.Errorf("recv auth response: %w", err)
	}

	switch msg.Type {
	case protocol.MsgTypeAuthOK:
		resp, err := protocol.Unmarshal[protocol.AuthResponse](msg.Payload)
		if err != nil {
			return fmt.Errorf("parse auth_ok: %w", err)
		}
		printBanner(resp.URL, c.cfg.LocalPort)

	case protocol.MsgTypeAuthErr:
		errResp, _ := protocol.Unmarshal[protocol.AuthError](msg.Payload)
		// Auth errors are fatal — do not reconnect.
		return fmt.Errorf("authentication failed: %s", errResp.Reason)

	default:
		return fmt.Errorf("unexpected message type %q", msg.Type)
	}

	// ---- Event loop ----
	for {
		msg, err := dec.Recv()
		if err != nil {
			return fmt.Errorf("control read: %w", err)
		}

		switch msg.Type {
		case protocol.MsgTypePing:
			if err := enc.Send(protocol.MsgTypePong, nil); err != nil {
				return fmt.Errorf("send pong: %w", err)
			}

		case protocol.MsgTypeProxy:
			req, err := protocol.Unmarshal[protocol.ProxyRequest](msg.Payload)
			if err != nil {
				log.Printf("[wackgrok] malformed proxy request: %v", err)
				continue
			}
			go c.handleProxy(req.RequestID)

		default:
			log.Printf("[wackgrok] unknown message type %q — ignoring", msg.Type)
		}
	}
}

// handleProxy opens a data connection to the server, sends the request ID to
// bind it to the in-flight HTTP request, then copies data bidirectionally
// between the server and the local service.
func (c *Client) handleProxy(requestID string) {
	// Dial server data port.
	dataConn, err := net.Dial("tcp", c.cfg.DataAddr)
	if err != nil {
		log.Printf("[proxy] %s: dial data port: %v", requestID, err)
		return
	}
	defer dataConn.Close()

	// Identify this connection to the server.
	if _, err := fmt.Fprintf(dataConn, "%s\n", requestID); err != nil {
		log.Printf("[proxy] %s: send request id: %v", requestID, err)
		return
	}

	// Dial the local service.
	localConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", c.cfg.LocalPort))
	if err != nil {
		log.Printf("[proxy] %s: dial local :%d: %v", requestID, c.cfg.LocalPort, err)
		return
	}
	defer localConn.Close()

	// Bidirectional copy; finish as soon as one side closes.
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		io.Copy(dst, src)
		// Signal EOF to the other direction.
		if tc, ok := dst.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}

	go cp(localConn, dataConn)
	go cp(dataConn, localConn)
	<-done
}

// ---- helpers ----

func printBanner(url string, localPort int) {
	fmt.Println()
	fmt.Println("  wackgrok tunnel established")
	fmt.Println()
	fmt.Printf("  Forwarding  %s  →  localhost:%d\n", url, localPort)
	fmt.Println()
	fmt.Println("  Press Ctrl+C to disconnect")
	fmt.Println()
}

// deriveDataAddr replaces the port in addr with port+1.
// E.g. "example.com:7070" → "example.com:7071".
func deriveDataAddr(addr string) string {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	return fmt.Sprintf("%s:%d", host, port+1)
}
