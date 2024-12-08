package block_manager

type BufferBlock struct {
	FileName    string
	BlockNumber uint32 // 0, 1, 2
	Data        []byte
}
