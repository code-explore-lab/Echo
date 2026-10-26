// Package model defines the Danmaku domain type and its validation rules.
package model

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"unicode/utf8"
)

// Danmaku is the canonical, persisted representation of one bullet message.
// It is produced by the hub (which assigns ID, Seq and TS) and consumed by
// both the broadcast path (as a protocol event) and the persistence path
// (where ID doubles as the MongoDB _id, making re-inserts idempotent).
type Danmaku struct {
	ID      string `json:"id" bson:"_id"`
	Room    string `json:"room" bson:"room"`
	Seq     int64  `json:"seq" bson:"seq"`
	UID     string `json:"uid" bson:"uid"`
	Content string `json:"content" bson:"content"`
	Color   string `json:"color" bson:"color"`
	Size    int    `json:"size" bson:"size"`
	TS      int64  `json:"ts" bson:"ts"` // unix milliseconds
}

const (
	MaxContentRunes = 100
	DefaultColor    = "#ffffff"
	DefaultSize     = 24
	MinSize         = 12
	MaxSize         = 64
)

// ValidateContent trims and rejects empty or over-long content.
func ValidateContent(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" || utf8.RuneCountInString(s) > MaxContentRunes {
		return "", false
	}
	return s, true
}

// NormalizeColor returns a valid #RGB/#RRGGBB color or the default.
func NormalizeColor(c string) string {
	c = strings.TrimSpace(c)
	if !validColor(c) {
		return DefaultColor
	}
	return strings.ToLower(c)
}

// NormalizeSize clamps the font size into [MinSize, MaxSize].
func NormalizeSize(s int) int {
	if s < MinSize || s > MaxSize {
		return DefaultSize
	}
	return s
}

func validColor(c string) bool {
	if !strings.HasPrefix(c, "#") {
		return false
	}
	hexPart := c[1:]
	if len(hexPart) != 3 && len(hexPart) != 6 {
		return false
	}
	for _, r := range hexPart {
		if !isHexDigit(r) {
			return false
		}
	}
	return true
}

func isHexDigit(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

// NewID returns a random 128-bit hex string, used both as the message id
// (returned in acks) and the MongoDB _id.
func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand never fails in practice; guard defensively.
		panic("model: cannot read random bytes: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// ShortID returns the first 8 hex chars of a fresh ID, suitable as a
// per-connection user id for logs and the join_ok greeting.
func ShortID() string {
	return NewID()[:8]
}
