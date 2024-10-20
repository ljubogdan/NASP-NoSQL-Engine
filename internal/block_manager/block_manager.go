package block_manager

type BlockManager struct {
	blockSize int
}

func NewBlockManager(blockSize int) *BlockManager {
	return &BlockManager{blockSize: blockSize}
}
