package block_manager

import (
	"NASP-NoSQL-Engine/internal/config"
	"container/list"
)

type BufferPool struct {
	Capacity uint32
	Pool     *list.List
}

func NewBufferPool() *BufferPool {

	capacity := config.ReadBufferPoolCapacity()
	return &BufferPool{
		Capacity: capacity,
		Pool: list.New(),
	}
}

func (bp *BufferPool) AddBlock(bb *BufferBlock) { // problem ako je bb == nil
	if bp.Pool.Len() == int(bp.Capacity) {
		bp.Pool.Remove(bp.Pool.Front())
	}

	bp.Pool.PushBack(bb)
}

func (bp *BufferPool) Clear() {
	bp.Pool.Init()
}

func (bp *BufferPool) GetBlock(fileName string, blockNumber uint32) *BufferBlock {
	for e := bp.Pool.Front(); e != nil; e = e.Next() {
		if e.Value.(*BufferBlock).FileName == fileName && e.Value.(*BufferBlock).BlockNumber == blockNumber {
			return e.Value.(*BufferBlock)
		}
	}
	return nil
}