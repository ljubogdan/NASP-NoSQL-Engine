package wal

import (
	"os"
	"log"
	"fmt"
	"encoding/json"
	"path/filepath"
)

const (
	ConfigPath = "../data/config.json"
	WalsPath = "../data/wals/"
)

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func DeleteOldWals() { // pronalazi low_watermark unutar config.json i prena njemu uklanja

	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)
	lwm := int(config["WAL"].(map[string]interface{})["low_watermark"].(float64))
	fmt.Println(lwm)

	for i := lwm; i >= 0; i-- {
		walFile := fmt.Sprintf("wal_%05d", i)
		walPath := filepath.Join(WalsPath, walFile)

		if _, err := os.Stat(walPath); os.IsNotExist(err) {
			continue
		}
		err := os.Remove(walPath)

		HandleError(err, "Failed to remove WAL file")
	}
	
}

func CreateWAL() *WAL{
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)
	bpw := uint32(config["WAL"].(map[string]interface{})["blocks_per_wal"].(float64))

	files, err := os.ReadDir(WalsPath)
	HandleError(err, "Failed to read wals directory")

	var walFile string

	if len(files) == 0 {
		walFile = "wal_00000"
	} else {
		walFile = files[len(files)-1].Name()
	}

	walPath := filepath.Join(WalsPath, walFile)
	wal := &WAL{
		BlocksPerWAL: bpw,
		Path: walPath,
	}

	return wal
}




