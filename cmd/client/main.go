// Command client is a minimal WebSocket load tester for MiniDanmaku. It opens
// -c concurrent connections to one room and sends -n messages each, then
// reports sent/acked/event counts, drops, and average ack latency.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type stats struct {
	sent   atomic.Int64
	acked  atomic.Int64
	events atomic.Int64
	errs   atomic.Int64
	latSum atomic.Int64
	latN   atomic.Int64
}

func main() {
	addr := flag.String("addr", "ws://localhost:8080/ws", "WebSocket endpoint (query appended)")
	room := flag.String("room", "100", "room id")
	token := flag.String("token", "", "auth token")
	conns := flag.Int("c", 1, "concurrent connections")
	msgs := flag.Int("n", 10, "messages per connection")
	flag.Parse()

	url := fmt.Sprintf("%s?room=%s&token=%s", *addr, *room, *token)

	var st stats
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runOne(url, *msgs, &st)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	sent, acked, events, errs := st.sent.Load(), st.acked.Load(), st.events.Load(), st.errs.Load()
	latSum, latN := st.latSum.Load(), st.latN.Load()

	fmt.Println("=== load test result ===")
	fmt.Printf("connections: %d\n", *conns)
	fmt.Printf("sent:        %d\n", sent)
	fmt.Printf("acked:       %d\n", acked)
	fmt.Printf("events:      %d\n", events)
	fmt.Printf("drops:       %d\n", sent-acked)
	fmt.Printf("dial errors: %d\n", errs)
	fmt.Printf("elapsed:     %v\n", elapsed)
	if sent > 0 {
		fmt.Printf("throughput:  %.0f msg/s\n", float64(sent)/elapsed.Seconds())
	}
	if latN > 0 {
		fmt.Printf("avg ack latency: %d ms\n", latSum/latN)
	}
}

func runOne(url string, n int, st *stats) {
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		st.errs.Add(1)
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		return
	}
	defer conn.Close()

	var mu sync.Mutex
	pending := make(map[string]time.Time)

	// Read goroutine: only it reads; only this goroutine's caller writes below.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var env struct {
				Type string `json:"type"`
				ID   string `json:"id"`
			}
			if err := json.Unmarshal(data, &env); err != nil {
				continue
			}
			switch env.Type {
			case "ack":
				st.acked.Add(1)
				mu.Lock()
				if t, ok := pending[env.ID]; ok {
					st.latSum.Add(time.Since(t).Milliseconds())
					st.latN.Add(1)
					delete(pending, env.ID)
				}
				mu.Unlock()
			case "danmaku":
				st.events.Add(1)
			}
		}
	}()

	for i := range n {
		id := randID()
		mu.Lock()
		pending[id] = time.Now()
		mu.Unlock()
		msg := map[string]any{
			"type":    "danmaku",
			"id":      id,
			"content": fmt.Sprintf("load-%d-%d", os.Getpid(), i),
			"color":   "#ffcc00",
			"size":    24,
		}
		if err := conn.WriteJSON(msg); err != nil {
			break
		}
		st.sent.Add(1)
	}

	// Give the server a moment to stream acks back before closing.
	time.Sleep(200 * time.Millisecond)
	_ = conn.Close()
	<-readDone
}

func randID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
