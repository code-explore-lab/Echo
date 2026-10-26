// Package protocol defines the JSON messages exchanged over the WebSocket
// and the REST history endpoint. Every frame is a JSON object with a "type".
package protocol

import "encoding/json"

// Message types (client -> server and server -> client).
const (
	TypeDanmaku = "danmaku" // client -> server: send a bullet
	TypeAck     = "ack"     // server -> client: accepted (NOT "persisted")
	TypeError   = "error"   // server -> client
	TypeJoinOK  = "join_ok" // server -> client: greeted on connect
	TypePing    = "ping"    // client -> server: app-level liveness (optional)
	TypePong    = "pong"    // server -> client
)

// Error codes.
const (
	CodeAuthFailed  = "AUTH_FAILED"
	CodeRateLimited = "RATE_LIMITED"
	CodeInvalid     = "INVALID"
	CodeInternal    = "INTERNAL"
)

// Inbound is the common envelope used to peek at the type before decoding.
type Inbound struct {
	Type string `json:"type"`
}

// ClientDanmaku is the payload a client sends. ID is the client-generated
// idempotency key: on timeout the client resends with the SAME id, and the
// server dedups it, returning the same MsgID.
type ClientDanmaku struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Content string `json:"content"`
	Color   string `json:"color"`
	Size    int    `json:"size"`
}

// DanmakuEvent is the authoritative broadcast pushed to every room member
// (including the sender). Clients render from this, not from the ack.
type DanmakuEvent struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Room    string `json:"room"`
	UID     string `json:"uid"`
	Content string `json:"content"`
	Color   string `json:"color"`
	Size    int    `json:"size"`
	TS      int64  `json:"ts"`
	Seq     int64  `json:"seq"`
}

// Ack acknowledges acceptance. It deliberately does NOT claim durability:
// OK means "accepted into the room and enqueued for persistence".
type Ack struct {
	Type  string `json:"type"`
	ID    string `json:"id"`    // echoed client idempotency key
	MsgID string `json:"msgId"` // server-assigned id, also the Mongo _id
	OK    bool   `json:"ok"`
}

// JoinOK greets a freshly connected client and reports the room's current
// sequence number so a reconnecting client knows where to resume.
type JoinOK struct {
	Type   string `json:"type"`
	Room   string `json:"room"`
	Client string `json:"client"`
	Seq    int64  `json:"seq"`
}

// ErrorMsg reports a rejection to the client.
type ErrorMsg struct {
	Type string `json:"type"`
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

// Encode marshals a message to its wire bytes. Encoding errors are impossible
// for these plain structs, so the error is swallowed deliberately.
func Encode(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
