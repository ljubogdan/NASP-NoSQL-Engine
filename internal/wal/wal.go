package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

type WAL struct {                          // ispade da nema nikakvu funkcionalnost zapravo, nije beskoristan skroz jer možemo izvući path
	BlocksPerWAL uint32
	Path         string
}

func NewWal() *WAL{
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
		walFilePath := filepath.Join(WalsPath, walFile)
		file, err := os.Create(walFilePath)
		HandleError(err, "Failed to create WAL file")
		file.Close()
	} else {
		sort.Slice(files, func(i, j int) bool {
			return files[i].Name() < files[j].Name()
		})
		walFile = files[len(files)-1].Name()
	}

	walPath := filepath.Join(WalsPath, walFile)
	wal := &WAL{
		BlocksPerWAL: bpw,
		Path: walPath,
	}

	return wal
}
