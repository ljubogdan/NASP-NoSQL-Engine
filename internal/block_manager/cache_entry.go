package block_manager

// za razliku od entry, cache entry ima:
// key
// value
// filename
// block number

// CacheEntry predstavlja jedan element u kešu
type CacheEntry struct {
	Key        string
	Value      []byte
	Filename   string
	BlockIndex uint32
}

// NewCacheEntry kreira novi CacheEntry
func NewCacheEntry(key string, value []byte, filename string, blockIndex uint32) *CacheEntry {
	return &CacheEntry{
		Key:        key,
		Value:      value,
		Filename:   filename,
		BlockIndex: blockIndex,
	}
}