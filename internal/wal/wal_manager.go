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

type WalManager struct {
	Wal *WAL
	LowWatermark uint32
}

func NewWalManager() *WalManager {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)
	lwm := uint32(config["WAL"].(map[string]interface{})["low_watermark"].(float64))

	wal := NewWal()
	return &WalManager{
		Wal: wal,
		LowWatermark: lwm,
	}
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func (wm *WalManager) DeleteOldWals() { // pronalazi low_watermark unutar config.json i prena njemu uklanja

	for i := int(wm.LowWatermark); i >= 0; i-- {
		walFile := fmt.Sprintf("wal_%05d", i)
		walPath := filepath.Join(WalsPath, walFile)

		if _, err := os.Stat(walPath); os.IsNotExist(err) {
			continue
		}
		err := os.Remove(walPath)

		HandleError(err, "Failed to remove WAL file")
	}
}


 








