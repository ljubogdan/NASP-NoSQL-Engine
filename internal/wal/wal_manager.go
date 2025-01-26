package wal

import (
	"NASP-NoSQL-Engine/internal/config"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

const (
	ConfigPath = "../data/config.json"
	WalsPath   = "../data/wals/"
)

type WalManager struct {
	Wal          *WAL
	LowWatermark uint32
}

func NewWalManager() *WalManager {

	lwm := config.ReadLowWatermark()

	wal := NewWal()
	return &WalManager{
		Wal:          wal,
		LowWatermark: lwm,
	}
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func (wm *WalManager) DeleteOldWals() { // pronalazi low_watermark unutar config.json i prena njemu uklanja

	// ako je low_watermark = 100000 (možemo imati u sistemu max 100000 wal fajlova (0, 1... 99999)) onda ne radimo ništa
	// razlog jer nije -1 jer koristimo uint32 i ne može biti negativan broj
	if wm.LowWatermark == 100000 {
		return
	}

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

func (wm *WalManager) SetLowWatermark(lwm uint32) {
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	config["WAL"].(map[string]interface{})["low_watermark"] = lwm

	prettyJSON, err := json.MarshalIndent(config, "", "    ")
	HandleError(err, "Failed to marshal JSON")

	err = os.WriteFile(ConfigPath, prettyJSON, 0644)
	HandleError(err, "Failed to write to config file")

	wm.LowWatermark = lwm
}
