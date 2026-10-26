package hub

import (
	"sync"
	"time"

	"minidanmaku/internal/model"
)

// 带容量上限 + TTL自动多起的幂等去重缓存
// dedupCache is a bounded, TTL-expiring map keyed by the client's idempotency
// key. Random eviction is sufficient here: the cache only needs to bound
// memory and remember resent ids for a short window, not enforce strict LRU.
type dedupCache struct {
	mu    sync.Mutex
	items map[string]dedupEntry
	max   int
	ttl   time.Duration
}

type dedupEntry struct {
	val       *model.Danmaku
	expiresAt time.Time
}

func newDedupCache(max int, ttl time.Duration) *dedupCache {
	return &dedupCache{items: make(map[string]dedupEntry), max: max, ttl: ttl}
}

// Get returns the cached danmaku for key, or false if absent or expired.
func (d *dedupCache) Get(key string) (*model.Danmaku, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.items[key]
	if !ok {
		return nil, false
	}
	// 惰性过期
	if time.Now().After(e.expiresAt) {
		delete(d.items, key)
		return nil, false
	}
	return e.val, true
}

// Add inserts a key; existing keys are left untouched and, when full, an
// arbitrary entry is evicted to keep the cache bounded.
func (d *dedupCache) Add(key string, val *model.Danmaku) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.items[key]; exists {
		return
	}
	if len(d.items) >= d.max {
		for k := range d.items {
			delete(d.items, k)
			break
		}
	}
	d.items[key] = dedupEntry{val: val, expiresAt: time.Now().Add(d.ttl)}
}
