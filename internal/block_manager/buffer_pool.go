package block_manager

import (
	"container/list"
	"NASP-NoSQL-Engine/internal/config"
)

// buffer pool kapacitet bi trebalo uvek da bude sinhronizovan sa kapacitetom wal-a
type BufferPool struct {
	Capacity uint32
	Pool     *list.List
}

func NewBufferPool() *BufferPool {

	capacity := config.ReadBufferPoolCapacity()

	return &BufferPool{
		Capacity: capacity,
		Pool:     list.New(),
	}
}

func (bp *BufferPool) AddBlock(bb *BufferBlock) {
	if bp.Pool.Len() == int(bp.Capacity) {
		bp.Pool.Remove(bp.Pool.Front())
	}

	bp.Pool.PushBack(bb)
}

func (bp *BufferPool) Clear() {
	bp.Pool.Init()
}
