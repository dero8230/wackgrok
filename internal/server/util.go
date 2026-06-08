package server

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"
)

var nonAlphanumDash = regexp.MustCompile(`[^a-z0-9-]`)

// randomID returns a lowercase hex string of length n.
func randomID(n int) string {
	b := make([]byte, (n/2)+1)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)[:n]
}

// sanitizeSubdomain strips characters that are invalid in a DNS label
// and truncates to 32 characters. Returns a random id if the result is empty.
func sanitizeSubdomain(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = nonAlphanumDash.ReplaceAllString(s, "")
	s = strings.Trim(s, "-")
	if len(s) > 32 {
		s = s[:32]
	}
	if s == "" {
		return randomID(8)
	}
	return s
}

// extractSubdomain returns the leftmost label of host when host ends with
// ".<domain>". Returns "" if host does not match or has nested sub-labels.
func extractSubdomain(host, domain string) string {
	suffix := "." + domain
	if !strings.HasSuffix(host, suffix) {
		return ""
	}
	sub := strings.TrimSuffix(host, suffix)
	if sub == "" || strings.Contains(sub, ".") {
		return ""
	}
	return sub
}
