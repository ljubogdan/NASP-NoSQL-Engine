package tokenbucket

import (
	"sync"
	"time"
)

type TokenBucket struct {
	capacity     int
	tokens       int
	fillInterval time.Duration
	lastRefill   time.Time
	mu           sync.Mutex
}

func NewTokenBucket(capacity int, fillInterval time.Duration) *TokenBucket {
	if capacity <= 0 {
		capacity = 1
	}
	return &TokenBucket{
		capacity:     capacity,
		tokens:       capacity,
		fillInterval: fillInterval,
		lastRefill:   time.Now(),
	}
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill)
	if elapsed <= 0 {
		return
	}

	tokensToAdd := int(elapsed / tb.fillInterval)
	if tokensToAdd > 0 {
		tb.tokens += tokensToAdd
		if tb.tokens > tb.capacity {
			tb.tokens = tb.capacity
		}
		tb.lastRefill = tb.lastRefill.Add(time.Duration(tokensToAdd) * tb.fillInterval)
	}
}

func (tb *TokenBucket) Allow(n int) bool {
	if n <= 0 {
		return true
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()
	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}
	return false
}
