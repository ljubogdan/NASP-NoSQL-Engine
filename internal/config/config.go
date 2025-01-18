package config

import (
	"encoding/json"
	"os"
	"log"
)

const (
	ConfigPath = "../data/config.json"
)

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func ReadBlockSize() uint32 {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	return uint32(config["SYSTEM"].(map[string]interface{})["block_size"].(map[string]interface{})["default"].(float64))
}

func ReadBlocksPerWal() uint32 {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	return uint32(config["WAL"].(map[string]interface{})["blocks_per_wal"].(float64))
}

func ReadBufferPoolCapacity() uint32 {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	return uint32(config["BUFFER_POOL"].(map[string]interface{})["capacity"].(float64))
}

func ReadCachePoolCapacity() uint32 {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	return uint32(config["CACHE_POOL"].(map[string]interface{})["capacity"].(float64))
}

