package block_manager

type CacheBlock struct {
	FileName    string // ime fajla za koji je blok vezan
	BlockNumber uint32 // redni broj bloka u fajlu
	Data        []byte // podaci bloka
}

func NewCacheBlock(fileName string, blockNumber uint32, data []byte) *CacheBlock {
	return &CacheBlock{
		FileName:    fileName,
		BlockNumber: blockNumber,
		Data:        data,
	}
}
