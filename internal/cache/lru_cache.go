package cache

type LRUCache struct {
	capacity int
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{capacity: capacity}
}
