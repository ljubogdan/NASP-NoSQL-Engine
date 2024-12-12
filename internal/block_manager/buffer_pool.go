package block_manager

import (
	"container/list"
	"encoding/json"
	"os"
)

type BufferPool struct {
	Capacity uint32
	Pool     *list.List
}

func NewBufferPool() *BufferPool {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)
	capacity := uint32(config["BUFFER_POOL"].(map[string]interface{})["capacity"].(float64))

	return &BufferPool{
		Capacity: capacity,
		Pool:     list.New(),
	}
}

func (bp *BufferPool) AddBlock(bb *BufferBlock) {
	if bp.Pool.Len() == int(bp.Capacity) {
		bp.Pool.Remove(bp.Pool.Front())
	}

	bp.Pool.PushBack(bb)
}

func (bp *BufferPool) Clear() {
	bp.Pool.Init()
}
