package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
)

// runHTTPServer starts the public-facing HTTP server that routes traffic to
// the appropriate tunnel based on the request's Host header.
func (s *Server) runHTTPServer(ctx context.Context) error {
	srv := &http.Server{
		Addr:    s.cfg.HTTPAddr,
		Handler: s,
	}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// ServeHTTP implements http.Handler. It:
//  1. Extracts the subdomain from the Host header.
//  2. Looks up the registered tunnel.
//  3. Signals the client to open a data connection.
//  4. Writes the HTTP request to the data connection.
//  5. Reads the HTTP response and writes it back to the caller.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	// Strip port suffix if present (e.g. "myapp.example.com:8080" → "myapp.example.com").
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	subdomain := extractSubdomain(host, s.cfg.Domain)
	if subdomain == "" {
		http.Error(w, "wackgrok: no tunnel found for this host: " + host, http.StatusNotFound)
		return
	}

	tunnel, ok := s.tunnels.Get(subdomain)
	if !ok {
		http.Error(w, fmt.Sprintf("wackgrok: tunnel %q is not connected", subdomain), http.StatusNotFound)
		return
	}

	requestID := randomID(16)

	// Ask the client to open a data connection.
	if err := tunnel.sendProxy(requestID); err != nil {
		log.Printf("[http] tunnel %s: sendProxy: %v", subdomain, err)
		http.Error(w, "wackgrok: failed to signal client", http.StatusBadGateway)
		return
	}

	// Wait for the client to dial back on the data port.
	dataConn, err := s.waitForConn(r.Context(), requestID)
	if err != nil {
		http.Error(w, "wackgrok: "+err.Error(), http.StatusGatewayTimeout)
		return
	}
	defer dataConn.Close()

	// Force HTTP/1.1 keep-alive off so we get a clean EOF after the response.
	r.Header.Set("Connection", "close")
	r.Close = true

	// Write the full HTTP request to the data connection.
	if err := r.Write(dataConn); err != nil {
		log.Printf("[http] tunnel %s: write request: %v", subdomain, err)
		http.Error(w, "wackgrok: failed to forward request", http.StatusBadGateway)
		return
	}

	// Read the HTTP response from the data connection.
	resp, err := http.ReadResponse(bufio.NewReader(dataConn), r)
	if err != nil {
		log.Printf("[http] tunnel %s: read response: %v", subdomain, err)
		http.Error(w, "wackgrok: failed to read response from client", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers.
	for key, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(key, v)
		}
	}
	// Ensure downstream connection is closed after each tunnelled response.
	w.Header().Set("Connection", "close")
	w.WriteHeader(resp.StatusCode)

	// Stream response body.
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("[http] tunnel %s: copy response body: %v", subdomain, err)
	}
}
