package block_manager

type Block struct {
	FileName    string
	BlockNumber uint32 // 0, 1, 2
	Data        []byte
}
