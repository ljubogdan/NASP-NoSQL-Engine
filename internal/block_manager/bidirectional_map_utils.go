package block_manager

import (
	"NASP-NoSQL-Engine/internal/config"
	"os"
	"io"
)

// ovde se nalaze funkcionalnosti tipa serijalizacija u fajl i deserijalizacija iz fajla

const (
	// samo će se serijalizovati u binaran fajl u data
	BidirectionalMapPath = "../data/bidirectional_map.bin" // globalni rečnik kompresije
)

// funkcija koja upisuje mapu u bin fajl
func (bm *BlockManager) WriteBidirectionalMapToFile() {
	// proveravamo da li postoji fajl na putanji, ako ne postoji pravimo novi
	if _, err := os.Stat(BidirectionalMapPath); os.IsNotExist(err) {
		_, err := os.Create(BidirectionalMapPath)
		HandleError(err, "Failed to create file")
	}

	file, err := os.OpenFile(BidirectionalMapPath, os.O_RDWR, 0644)
	HandleError(err, "Failed to open file")

	// brišemo totalni sadržaj fajla
	err = file.Truncate(0)
	HandleError(err, "Failed to truncate file")

	// serijalizujemo mapu
	serialized := bm.BidirectionalMap.SerializeBidirectionalMap()

	// upisujemo u fajl blokovski
	// NAPOMENA: ne sme se dodavati padding jer će se onda deserializacija poremetiti
	blockSize := config.ReadBlockSize()
	for i := 0; i < len(serialized); i += int(blockSize) {
		end := i + int(blockSize)
		if end > len(serialized) {
			end = len(serialized)
		}

		_, err = file.Write(serialized[i:end])
		HandleError(err, "Failed to write to file")
	}

	err = file.Close()
	HandleError(err, "Failed to close file")
}

// funkcija koja čita mapu iz bin fajla
func (bm *BlockManager) ReadBidirectionalMapFromFile() {
	// mapa mora postojati na putanji, ako ne postoji onda je prazna i bacimo error
	if _, err := os.Stat(BidirectionalMapPath); os.IsNotExist(err) {
		// kreiramo novu praznu mapu i upisujemo je u fajl
		bm.BidirectionalMap = NewBidirectionalMap()
		bm.WriteBidirectionalMapToFile()
	}

	file, err := os.Open(BidirectionalMapPath)
	HandleError(err, "Failed to open file")

	// čitamo sve bajtove iz fajla BLOKOVSKI
	// NAPOMENA: ne sme se dodavati padding jer će se onda deserializacija poremetiti
	blockSize := config.ReadBlockSize()
	data := make([]byte, blockSize)

	bdmapBytes := make([]byte, 0)

	for {
		n, err := file.Read(data)
		if n > 0 {
			bdmapBytes = append(bdmapBytes, data[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			HandleError(err, "Failed to read from file")
		}
	}
	
	// deserijalizujemo mapu
	bm.BidirectionalMap = DeserializeBidirectionalMap(bdmapBytes)

	err = file.Close()
	HandleError(err, "Failed to close file")
}
