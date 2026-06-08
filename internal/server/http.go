package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"
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
	start := time.Now()

	host := r.Host
	// Strip port suffix if present (e.g. "myapp.example.com:8080" → "myapp.example.com").
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	subdomain := extractSubdomain(host, s.cfg.Domain)
	if subdomain == "" {
		log.Printf("[http] no tunnel for host=%q", host)
		http.Error(w, "wackgrok: no tunnel found for this host: "+host, http.StatusNotFound)
		return
	}

	tunnel, ok := s.tunnels.Get(subdomain)
	if !ok {
		log.Printf("[http] tunnel %q not connected (host=%s)", subdomain, host)
		http.Error(w, fmt.Sprintf("wackgrok: tunnel %q is not connected", subdomain), http.StatusNotFound)
		return
	}

	requestID := randomID(16)
	log.Printf("[http] req=%s  %s %s  tunnel=%s", requestID, r.Method, r.URL.RequestURI(), subdomain)

	// ---- Step 1: signal client ----
	if err := tunnel.sendProxy(requestID); err != nil {
		log.Printf("[http] req=%s  sendProxy failed: %v", requestID, err)
		http.Error(w, "wackgrok: failed to signal client", http.StatusBadGateway)
		return
	}
	log.Printf("[http] req=%s  proxy signal sent, waiting for data conn…", requestID)

	// ---- Step 2: wait for client data connection ----
	dataConn, err := s.waitForConn(r.Context(), requestID)
	if err != nil {
		log.Printf("[http] req=%s  waitForConn failed after %s: %v", requestID, time.Since(start), err)
		http.Error(w, "wackgrok: "+err.Error(), http.StatusGatewayTimeout)
		return
	}
	defer dataConn.Close()
	log.Printf("[http] req=%s  data conn established (%s)", requestID, time.Since(start))

	// Force HTTP/1.1 keep-alive off so we get clean framing on the data conn.
	r.Header.Set("Connection", "close")
	r.Close = true

	// ---- Step 3: forward request to client ----
	if err := r.Write(dataConn); err != nil {
		log.Printf("[http] req=%s  write request failed: %v", requestID, err)
		http.Error(w, "wackgrok: failed to forward request", http.StatusBadGateway)
		return
	}
	log.Printf("[http] req=%s  request written to data conn (%s)", requestID, time.Since(start))

	// ---- Step 4: read response from client ----
	resp, err := http.ReadResponse(bufio.NewReader(dataConn), r)
	if err != nil {
		log.Printf("[http] req=%s  ReadResponse failed after %s: %v", requestID, time.Since(start), err)
		http.Error(w, "wackgrok: failed to read response from client", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	log.Printf("[http] req=%s  got response status=%d (%s)", requestID, resp.StatusCode, time.Since(start))

	// ---- Step 5: relay response to browser ----
	for key, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(key, v)
		}
	}
	w.Header().Set("Connection", "close")
	w.WriteHeader(resp.StatusCode)

	n, err := io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("[http] req=%s  copy body failed after %d bytes: %v", requestID, n, err)
		return
	}
	log.Printf("[http] req=%s  done — %d bytes in %s", requestID, n, time.Since(start))
}
