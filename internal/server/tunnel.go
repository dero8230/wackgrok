package server

import (
	"sync"

	"github.com/paskhal/wackgrok/internal/protocol"
)

// Tunnel represents a single active client tunnel.
type Tunnel struct {
	ID string

	mu  sync.Mutex
	enc *protocol.Encoder
}

// sendProxy signals the client to open a data connection for requestID.
func (t *Tunnel) sendProxy(requestID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.enc.Send(protocol.MsgTypeProxy, protocol.ProxyRequest{RequestID: requestID})
}

// sendPing sends a ping over the control channel.
func (t *Tunnel) sendPing() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.enc.Send(protocol.MsgTypePing, nil)
}

// TunnelRegistry is a concurrency-safe map from subdomain id → Tunnel.
type TunnelRegistry struct {
	mu      sync.RWMutex
	tunnels map[string]*Tunnel
}

func newTunnelRegistry() *TunnelRegistry {
	return &TunnelRegistry{tunnels: make(map[string]*Tunnel)}
}

// Register adds t under id. Returns false if id is already taken.
func (r *TunnelRegistry) Register(id string, t *Tunnel) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tunnels[id]; exists {
		return false
	}
	r.tunnels[id] = t
	return true
}

// Unregister removes the tunnel for id.
func (r *TunnelRegistry) Unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tunnels, id)
}

// Get returns the tunnel for id, or false if not found.
func (r *TunnelRegistry) Get(id string) (*Tunnel, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tunnels[id]
	return t, ok
}
