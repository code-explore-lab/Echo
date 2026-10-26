package model

import "testing"

func TestValidateContent(t *testing.T) {
	if got, ok := ValidateContent("  hi  "); !ok || got != "hi" {
		t.Fatalf("got %q, %v; want trimmed valid", got, ok)
	}
	if _, ok := ValidateContent("   "); ok {
		t.Fatal("whitespace-only should be invalid")
	}
	long := make([]rune, MaxContentRunes+1)
	for i := range long {
		long[i] = 'a'
	}
	if _, ok := ValidateContent(string(long)); ok {
		t.Fatal("over-long content should be invalid")
	}
}

func TestNormalizeColor(t *testing.T) {
	if got := NormalizeColor("not-a-color"); got != DefaultColor {
		t.Fatalf("got %q, want default", got)
	}
	if got := NormalizeColor("#FF00AA"); got != "#ff00aa" {
		t.Fatalf("got %q, want lowercased", got)
	}
	if got := NormalizeColor("#abc"); got != "#abc" {
		t.Fatalf("got %q, want #abc preserved", got)
	}
}

func TestNormalizeSize(t *testing.T) {
	if NormalizeSize(0) != DefaultSize {
		t.Fatal("too-small size should fall back to default")
	}
	if NormalizeSize(999) != DefaultSize {
		t.Fatal("too-large size should fall back to default")
	}
	if NormalizeSize(30) != 30 {
		t.Fatal("in-range size should pass through")
	}
}
