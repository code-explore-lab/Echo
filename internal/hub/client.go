package hub

import (
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"

	"minidanmaku/internal/model"
	"minidanmaku/internal/protocol"
)

// Persister is the narrow sink the hub uses to hand a validated danmaku to the
// async persistence path. It must never block: Enqueue returns false on
// overflow instead of waiting.
type Persister interface {
	Enqueue(d *model.Danmaku) bool
}

// pongFrame is the canned response to an app-level ping.
var pongFrame = []byte(`{"type":"pong"}`)

// Client is a single WebSocket connection. Exactly two goroutines touch it:
// readPump (reads + handles inbound messages) and writePump (the ONLY writer).
type Client struct {
	room string
	uid  string
	conn *websocket.Conn
	send chan []byte // bounded outbound buffer consumed by writePump

	roomRef Room
	persist Persister
	limiter *rate.Limiter

	pingPeriod time.Duration
	pongWait   time.Duration
	writeWait  time.Duration
	maxMsgSize int64
	maxDrops   int64

	log *slog.Logger

	closeOnce sync.Once
	done      chan struct{} // closed once, signals teardown
	dropped   atomic.Int64
}

// ClientOptions carries everything NewClient needs; keeps the constructor
// readable as the number of knobs grows.
type ClientOptions struct {
	Room       string
	UID        string
	Conn       *websocket.Conn
	RoomRef    Room
	Persist    Persister
	Limiter    *rate.Limiter
	SendBuf    int
	PingPeriod time.Duration
	PongWait   time.Duration
	WriteWait  time.Duration
	MaxMsgSize int64
	MaxDrops   int64
	Logger     *slog.Logger
}

// NewClient builds a Client. An optional nil limiter means "unlimited".
func NewClient(opts ClientOptions) *Client {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Limiter == nil {
		opts.Limiter = rate.NewLimiter(rate.Inf, 0)
	}
	return &Client{
		room:       opts.Room,
		uid:        opts.UID,
		conn:       opts.Conn,
		send:       make(chan []byte, opts.SendBuf),
		roomRef:    opts.RoomRef,
		persist:    opts.Persist,
		limiter:    opts.Limiter,
		pingPeriod: opts.PingPeriod,
		pongWait:   opts.PongWait,
		writeWait:  opts.WriteWait,
		maxMsgSize: opts.MaxMsgSize,
		maxDrops:   opts.MaxDrops,
		log:        opts.Logger,
		done:       make(chan struct{}),
	}
}

// Start joins the room, greets the client, and launches the two pumps.
func (c *Client) Start() {
	c.roomRef.Join(c)
	c.sendJoinOK()
	go c.writePump()
	go c.readPump()
}

// writePump is the single writer for the connection. It multiplexes outbound
// messages with WS-level pings; no other goroutine ever calls WriteMessage.
func (c *Client) writePump() {
	defer c.close()
	ticker := time.NewTicker(c.pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case msg := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			// 发心跳包的
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

// readPump reads inbound frames and dispatches them. It enforces the read
// limit and heartbeat (read deadline refreshed on pong). On any error it
// leaves the room and tears the connection down.
func (c *Client) readPump() {
	defer func() {
		// 退出， 从map中移除， 退出channel
		c.roomRef.Leave(c)
		c.close()
	}()

	c.conn.SetReadLimit(c.maxMsgSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(c.pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.pongWait))
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				c.log.Debug("client disconnected", "uid", c.uid, "room", c.room, "err", err)
			}
			return
		}
		c.handleMessage(data)
	}
}

// handleMessage peeks at the type and dispatches.
func (c *Client) handleMessage(data []byte) {
	var in protocol.Inbound
	if err := json.Unmarshal(data, &in); err != nil {
		c.sendError(protocol.CodeInvalid, "invalid JSON")
		return
	}
	switch in.Type {
	case protocol.TypeDanmaku:
		c.handleDanmaku(data)
	case protocol.TypePing:
		c.trySend(pongFrame)
	default:
		c.sendError(protocol.CodeInvalid, "unknown message type")
	}
}

// handleDanmaku validates, rate-limits, dedups, broadcasts and persists a
// bullet. Broadcast and persistence are both fire-and-forget.
func (c *Client) handleDanmaku(data []byte) {
	var in protocol.ClientDanmaku
	if err := json.Unmarshal(data, &in); err != nil {
		c.sendError(protocol.CodeInvalid, "invalid danmaku")
		return
	}

	// Shed spam cheaply before doing any real work.
	if !c.limiter.Allow() {
		c.sendError(protocol.CodeRateLimited, "rate limit exceeded")
		return
	}

	content, ok := model.ValidateContent(in.Content)
	if !ok {
		c.sendError(protocol.CodeInvalid, "content empty or too long")
		return
	}
	if in.ID == "" {
		c.sendError(protocol.CodeInvalid, "missing id")
		return
	}
	color := model.NormalizeColor(in.Color)
	size := model.NormalizeSize(in.Size)

	d, dup := c.roomRef.Ingest(c, in.ID, content, color, size, time.Now().UnixMilli())

	// Always ack — even on a duplicate — so a resending client converges on
	// the SAME server msgId and never double-broadcasts.
	c.trySend(protocol.Encode(protocol.Ack{
		Type:  protocol.TypeAck,
		ID:    in.ID,
		MsgID: d.ID,
		OK:    true,
	}))

	if dup {
		return
	}

	event := protocol.DanmakuEvent{
		Type:    protocol.TypeDanmaku,
		ID:      d.ID,
		Room:    d.Room,
		UID:     d.UID,
		Content: d.Content,
		Color:   d.Color,
		Size:    d.Size,
		TS:      d.TS,
		Seq:     d.Seq,
	}
	c.roomRef.Broadcast(protocol.Encode(event))

	// Persistence never blocks the room broadcast. Dropping here is explicit:
	// we log it and move on, rather than pretending it was stored.
	if !c.persist.Enqueue(d) {
		c.log.Warn("persist queue rejected", "id", d.ID, "room", d.Room)
	}
}

// trySend enqueues msg for the writePump without blocking. On a full buffer it
// drops the message (newest-first) and, once drops exceed the threshold,
// disconnects the persistently-slow consumer so it can reconnect cleanly.
func (c *Client) trySend(msg []byte) bool {
	select {
	case c.send <- msg:
		return true
	case <-c.done:
		return false
	default:
		// 累计丢包数 > 最大允许丢包maxDrops
		if c.dropped.Add(1) >= c.maxDrops {
			c.log.Warn("disconnecting slow consumer", "uid", c.uid, "room", c.room, "dropped", c.dropped.Load())
			c.close()
		}
		return false
	}
}

func (c *Client) sendJoinOK() {
	c.trySend(protocol.Encode(protocol.JoinOK{
		Type:   protocol.TypeJoinOK,
		Room:   c.room,
		Client: c.uid,
		Seq:    c.roomRef.Seq(),
	}))
}

func (c *Client) sendError(code, msg string) {
	c.trySend(protocol.Encode(protocol.ErrorMsg{Type: protocol.TypeError, Code: code, Msg: msg}))
}

// close idempotently tears the connection down: it closes done (stopping the
// writePump) and the socket (unblocking the readPump). The send channel is
// intentionally never closed — trySend guards with done instead, so no goroutine
// can panic on a send-after-close.
func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}
