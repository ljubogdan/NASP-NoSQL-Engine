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

func (cp *CachePool) Clear() {
	cp.Pool.Init()
}

func (cp *CachePool) GetBlock(name string, blockNumber uint32) *CacheBlock {
	for e := cp.Pool.Front(); e != nil; e = e.Next() {
		if e.Value.(*CacheBlock).FileName == name && e.Value.(*CacheBlock).BlockNumber == blockNumber {
			return e.Value.(*CacheBlock)
		}
	}
	return nil
}
