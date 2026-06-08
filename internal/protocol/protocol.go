// Package protocol defines the shared message types and encoding helpers
// used on the control channel between the wackgrok server and client.
package protocol

import (
	"encoding/json"
	"io"
)

// Message type constants.
const (
	MsgTypeAuth    = "auth"
	MsgTypeAuthOK  = "auth_ok"
	MsgTypeAuthErr = "auth_err"
	MsgTypeProxy   = "proxy"
	MsgTypePing    = "ping"
	MsgTypePong    = "pong"
)

// Message is the envelope for every control-channel message.
// The Payload field is decoded into the appropriate struct based on Type.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// AuthRequest is the first message sent by the client after connecting.
type AuthRequest struct {
	Token     string `json:"token"`
	Subdomain string `json:"subdomain,omitempty"` // optional; server falls back to random
}

// AuthResponse is sent by the server on a successful authentication.
type AuthResponse struct {
	ID  string `json:"id"`  // assigned subdomain id
	URL string `json:"url"` // full tunnel URL
}

// AuthError is sent by the server when authentication fails.
type AuthError struct {
	Reason string `json:"reason"`
}

// ProxyRequest is sent by the server to ask the client to open a data
// connection for a specific in-flight HTTP request.
type ProxyRequest struct {
	RequestID string `json:"request_id"`
}

// ---- Encoder / Decoder helpers ----

// Encoder writes JSON-encoded messages to an underlying writer.
type Encoder struct {
	enc *json.Encoder
}

// NewEncoder wraps w in an Encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{enc: json.NewEncoder(w)}
}

// Send marshals payload and writes it as a Message with the given type.
// A nil payload is allowed (e.g. for ping/pong).
func (e *Encoder) Send(msgType string, payload any) error {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		raw = b
	}
	return e.enc.Encode(Message{Type: msgType, Payload: raw})
}

// Decoder reads JSON-encoded messages from an underlying reader.
type Decoder struct {
	dec *json.Decoder
}

// NewDecoder wraps r in a Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{dec: json.NewDecoder(r)}
}

// Recv blocks until the next message is available or an error occurs.
func (d *Decoder) Recv() (Message, error) {
	var msg Message
	err := d.dec.Decode(&msg)
	return msg, err
}

// Unmarshal decodes a raw JSON payload into T.
func Unmarshal[T any](raw json.RawMessage) (T, error) {
	var v T
	err := json.Unmarshal(raw, &v)
	return v, err
}
