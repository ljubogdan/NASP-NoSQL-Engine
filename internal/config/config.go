package config

import (
	"encoding/json"
	"log"
	"os"
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

func ReadLowWatermark() uint32 {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)
	return uint32(config["WAL"].(map[string]interface{})["low_watermark"].(float64))
}

func ReadSummaryThinning() uint32 {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	return uint32(config["SSTABLE"].(map[string]interface{})["SSTABLE_SUMMARY"].(map[string]interface{})["thinning"].(float64))
}

func ReadBloomFilterExpectedElements() uint32 {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	return uint32(config["BLOOM_FILTER"].(map[string]interface{})["expected_elements"].(float64))
}

func ReadBloomFilterFalsePositiveRate() float64 {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	return config["BLOOM_FILTER"].(map[string]interface{})["false_positive_rate"].(float64)
}

func ReadMerge() bool {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	return config["SSTABLE"].(map[string]interface{})["MERGE"].(bool)
}

func WriteLowWatermark(lowWatermark uint32) {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	config["WAL"].(map[string]interface{})["low_watermark"] = lowWatermark

	newConfig, err := json.MarshalIndent(config, "", "  ")
	HandleError(err, "Failed to marshal config")

	err = os.WriteFile(ConfigPath, newConfig, 0644)
	HandleError(err, "Failed to write config file")
}
