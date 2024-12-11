package block_manager

type BufferBlock struct {
	FileName    string
	BlockNumber uint32 // 0, 1, 2
	Data        []byte
}

func NewBufferBlock(fileName string, blockNumber uint32, data []byte) *BufferBlock {
	return &BufferBlock{
		FileName:    fileName,
		BlockNumber: blockNumber,
		Data:        data,
	}
}
