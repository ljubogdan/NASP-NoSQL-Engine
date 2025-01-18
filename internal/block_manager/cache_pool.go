package block_manager

import (
	"container/list"
	"NASP-NoSQL-Engine/internal/config"
)

type CachePool struct {
	Capacity uint32
	Pool     *list.List
}

func NewCachePool() *CachePool {

	capacity := config.ReadCachePoolCapacity()

	return &CachePool{
		Capacity: capacity,
		Pool:     list.New(),
	}
}

func (cp *CachePool) AddBlock(cb *CacheBlock) {
	if cp.Pool.Len() == int(cp.Capacity) {
		cp.Pool.Remove(cp.Pool.Front())
	}

	cp.Pool.PushBack(cb)
}
