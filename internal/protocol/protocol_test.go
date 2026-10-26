package protocol

import (
	"encoding/json"
	"testing"
)

func TestAckRoundTrip(t *testing.T) {
	ack := Ack{Type: TypeAck, ID: "client-1", MsgID: "msg-1", OK: true}
	var out Ack
	if err := json.Unmarshal(Encode(ack), &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != "client-1" || out.MsgID != "msg-1" || !out.OK {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestDanmakuEventRoundTrip(t *testing.T) {
	ev := DanmakuEvent{Type: TypeDanmaku, ID: "m1", Room: "100", UID: "u1", Content: "hi", Color: "#fff", Size: 24, TS: 123, Seq: 5}
	var out DanmakuEvent
	if err := json.Unmarshal(Encode(ev), &out); err != nil {
		t.Fatal(err)
	}
	if out.Seq != 5 || out.TS != 123 || out.Content != "hi" {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}
