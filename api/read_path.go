package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
)

type ReadPath struct {
	BlockManager    *block_manager.BlockManager
	MemtableManager *memtable.MemtableManager
}

func NewReadPath(blockManager *block_manager.BlockManager, memtableManager *memtable.MemtableManager) *ReadPath {
	return &ReadPath{
		BlockManager:    blockManager,
		MemtableManager: memtableManager,
	}
}

func (rpo *ReadPath) ReadEntry(key string) (*entry.Entry, bool) {
	result, exists := rpo.MemtableManager.Find(key)
	return &result, exists
}
