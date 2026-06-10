package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/paskhal/wackgrok/internal/client"
)

func main() {
	server := flag.String("server", envOr("WACKGROK_SERVER", "wackgrok.paskhal.com:7070"), "server address (host:port, e.g. example.com:7070)")
	dataAddr := flag.String("data", envOr("WACKGROK_DATA_ADDR", ""), "server data address (optional; derived from -server if omitted)")
	token := flag.String("token", envOr("WACKGROK_TOKEN", ""), "authentication token")
	port := flag.Int("port", 0, "local port to expose (required)")
	subdomain := flag.String("subdomain", "", "requested subdomain (optional; random if omitted)")
	flag.Parse()

	if *server == "" || *token == "" || *port == 0 {
		fmt.Fprintf(os.Stderr, `Usage: wackgrok-client [flags]

  -server    HOST:PORT   server control address  (or WACKGROK_SERVER)
  -token     TOKEN       authentication token    (or WACKGROK_TOKEN)
  -port      PORT        local port to expose    (required)
  -subdomain NAME        requested subdomain     (optional)
  -data      HOST:PORT   override data address   (optional)

Examples:
  wackgrok-client -server example.com:7070 -token secret -port 3000
  wackgrok-client -server example.com:7070 -token secret -port 8080 -subdomain myapp
`)
		os.Exit(1)
	}

	cfg := client.Config{
		ServerAddr: *server,
		DataAddr:   *dataAddr,
		Token:      *token,
		LocalPort:  *port,
		Subdomain:  *subdomain,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Auth errors (wrong token etc.) are returned as non-nil and are fatal.
	if err := client.New(cfg).Run(ctx); err != nil {
		log.Fatalf("wackgrok-client: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
