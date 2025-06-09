package tokenbucket

import (
	"testing"
	"time"
)

func TestAllow(t *testing.T) {
	tb := NewTokenBucket(2, time.Millisecond*100)

	if !tb.Allow(1) {
		t.Errorf("očekivano je da prvi token bude dozvoljen")
	}
	if !tb.Allow(1) {
		t.Errorf("očekivano je da drugi token bude dozvoljen")
	}
	if tb.Allow(1) {
		t.Errorf("očekivano je da treći token bude odbijen")
	}

	time.Sleep(120 * time.Millisecond)

	if !tb.Allow(1) {
		t.Errorf("očekivano je da token nakon dopune bude dozvoljen")
	}
}
