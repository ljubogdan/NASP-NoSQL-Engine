package block_manager

import (
	"log"
)

const (
	ConfigPath = "../data/config.json"
)

type BlockManager struct {
	BufferPool *BufferPool
	CachePool  *CachePool
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func NewBlockManager() *BlockManager {
	return &BlockManager{
		BufferPool: NewBufferPool(),
		CachePool:  NewCachePool(),
	}
}




