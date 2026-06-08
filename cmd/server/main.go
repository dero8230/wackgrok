package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/paskhal/wackgrok/internal/server"
)

func main() {
	controlAddr := flag.String("control", envOr("WACKGROK_CONTROL", ":7070"), "control listener address")
	dataAddr := flag.String("data", envOr("WACKGROK_DATA", ":7071"), "data listener address")
	httpAddr := flag.String("http", envOr("WACKGROK_HTTP", ":80"), "HTTP listener address")
	domain := flag.String("domain", envOr("WACKGROK_DOMAIN", "wackgrok.paskhal.com"), "base domain for tunnel subdomains")
	tokensFlag := flag.String("tokens", envOr("WACKGROK_TOKENS", ""), "comma-separated list of valid auth tokens")
	flag.Parse()

	if *tokensFlag == "" {
		log.Fatal("no tokens configured — set -tokens or WACKGROK_TOKENS")
	}

	tokens := make(map[string]bool)
	for _, t := range strings.Split(*tokensFlag, ",") {
		if t = strings.TrimSpace(t); t != "" {
			tokens[t] = true
		}
	}

	cfg := server.Config{
		ControlAddr: *controlAddr,
		DataAddr:    *dataAddr,
		HTTPAddr:    *httpAddr,
		Domain:      *domain,
		Tokens:      tokens,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := server.New(cfg).Run(ctx); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
