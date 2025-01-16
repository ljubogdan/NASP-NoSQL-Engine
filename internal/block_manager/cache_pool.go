package block_manager

import (
	"container/list"
	"encoding/json"
	"os"
)

type CachePool struct {
	Capacity uint32
	Pool     *list.List
}

func NewCachePool() *CachePool {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	capacity := uint32(config["CACHE_POOL"].(map[string]interface{})["capacity"].(float64))

	return &CachePool{
		Capacity: capacity,
		Pool:     list.New(),
	}
}

func (cp *CachePool) AddBlock(cb *CacheBlock) {
	if cp.Pool.Len() == int(cp.Capacity) {
		cp.Pool.Remove(cp.Pool.Front())
	}

	cp.Pool.PushBack(cb)
}
