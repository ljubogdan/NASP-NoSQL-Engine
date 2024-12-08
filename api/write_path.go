package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/wal"
)

type WritePath struct {
	BlockManager *block_manager.BlockManager
	Wal          *wal.WalManager
}

func NewWritePath() *WritePath {
	return &WritePath{
		BlockManager: block_manager.NewBlockManager(),
		Wal:          wal.NewWalManager(),
	}
}

func (wpo *WritePath) WriteEntry(key string, value string) {
	// Write to wal
}
