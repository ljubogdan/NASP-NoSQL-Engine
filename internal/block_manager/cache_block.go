package block_manager

type CacheBlock struct {
	FileName    string // ime fajla za koji je blok vezan
	BlockNumber uint32 // redni broj bloka u fajlu
	Data        []byte // podaci bloka
	BlockSize   uint32 // veličina bloka
}

func NewCacheBlock(fileName string, blockNumber uint32, data []byte, blockSize uint32) *CacheBlock {
	return &CacheBlock{
		FileName:    fileName, // ime sstable_xxxxx + data, blocksize, merge, toc...
		BlockNumber: blockNumber,
		Data:        data,
		BlockSize:   blockSize,
	}
}


