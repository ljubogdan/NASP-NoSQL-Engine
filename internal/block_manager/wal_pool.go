package block_manager

import (
	"container/list"
	"NASP-NoSQL-Engine/internal/config"
)

// buffer pool kapacitet bi trebalo uvek da bude sinhronizovan sa kapacitetom wal-a
type WalPool struct {
	Capacity uint32
	Pool     *list.List
}

func NewWalPool() *WalPool {

	capacity := config.ReadWalPoolCapacity()

	return &WalPool{
		Capacity: capacity,
		Pool:     list.New(),
	}
}

func (bp *WalPool) AddBlock(bb *BufferBlock) {
	if bp.Pool.Len() == int(bp.Capacity) {
		bp.Pool.Remove(bp.Pool.Front())
	}

	bp.Pool.PushBack(bb)
}

func (bp *WalPool) Clear() {
	bp.Pool.Init()
}

func (bp *WalPool) GetBlock(name string, blockNumber uint32) *BufferBlock {
	for e := bp.Pool.Front(); e != nil; e = e.Next() {
		if e.Value.(*BufferBlock).FileName == name && e.Value.(*BufferBlock).BlockNumber == blockNumber {
			return e.Value.(*BufferBlock)
		}
	}
	return nil
}
