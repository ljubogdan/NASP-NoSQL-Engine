package block_manager

type BufferBlock struct {
	FileName    string
	BlockNumber uint32 // 0, 1, 2
	Data        []byte
	BlockSize   uint32

	WrittenStatus bool
}

func NewBufferBlock(fileName string, blockNumber uint32, data []byte, blockSize uint32, writtenStatus bool) *BufferBlock {
	return &BufferBlock{
		FileName:    fileName,
		BlockNumber: blockNumber,
		Data:        data,
		BlockSize:   blockSize,
		
		WrittenStatus: writtenStatus,
	}
}
