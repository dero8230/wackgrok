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
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
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

	// Close conn when ctx is cancelled so that any blocking read below
	// returns immediately instead of hanging until the next packet.
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

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
			if ctx.Err() != nil {
				return nil // context cancelled — clean shutdown, don't reconnect
			}
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

// handleProxy opens a data connection to the server, reads the HTTP request
// the server wrote, forwards it to the local service, and writes the response
// back. Using HTTP-level forwarding (rather than raw bidirectional copy) means
// the response is properly framed with Content-Length / chunked encoding, so
// neither side needs a connection-close EOF to know when the body ends.
func (c *Client) handleProxy(requestID string) {
	start := time.Now()
	log.Printf("[proxy] req=%s  start", requestID)

	// ---- Step 1: dial server data port ----
	dataConn, err := net.Dial("tcp", c.cfg.DataAddr)
	if err != nil {
		log.Printf("[proxy] req=%s  dial data port %s: %v", requestID, c.cfg.DataAddr, err)
		return
	}
	defer dataConn.Close()
	log.Printf("[proxy] req=%s  data conn open (%s)", requestID, time.Since(start))

	// ---- Step 2: identify this connection to the server ----
	if _, err := fmt.Fprintf(dataConn, "%s\n", requestID); err != nil {
		log.Printf("[proxy] req=%s  send request id: %v", requestID, err)
		return
	}
	log.Printf("[proxy] req=%s  request id sent (%s)", requestID, time.Since(start))

	// ---- Step 3: read the HTTP request the server wrote ----
	req, err := http.ReadRequest(bufio.NewReader(dataConn))
	if err != nil {
		log.Printf("[proxy] req=%s  ReadRequest failed: %v", requestID, err)
		return
	}
	defer req.Body.Close()
	log.Printf("[proxy] req=%s  read request: %s %s (%s)", requestID, req.Method, req.RequestURI, time.Since(start))

	// ---- Step 4: re-target at the local service ----
	req.URL.Scheme = "http"
	req.URL.Host = fmt.Sprintf("127.0.0.1:%d", c.cfg.LocalPort)
	req.RequestURI = "" // must be empty for outbound requests

	// ---- Step 5: forward to local service ----
	log.Printf("[proxy] req=%s  forwarding to localhost:%d…", requestID, c.cfg.LocalPort)
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		log.Printf("[proxy] req=%s  local :%d error: %v", requestID, c.cfg.LocalPort, err)
		msg := fmt.Sprintf("wackgrok: local service unreachable: %v", err)
		fmt.Fprintf(dataConn, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
			len(msg), msg)
		return
	}
	defer resp.Body.Close()
	log.Printf("[proxy] req=%s  local responded status=%d (%s)", requestID, resp.StatusCode, time.Since(start))

	// ---- Step 6: write response back to server ----
	// resp.Write uses Content-Length / chunked framing — no EOF sentinel needed.
	log.Printf("[proxy] req=%s  writing response to server…", requestID)
	if err := resp.Write(dataConn); err != nil {
		log.Printf("[proxy] req=%s  write response failed: %v", requestID, err)
		return
	}
	log.Printf("[proxy] req=%s  done (%s)", requestID, time.Since(start))
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
