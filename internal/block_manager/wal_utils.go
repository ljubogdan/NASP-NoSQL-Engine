package block_manager

import (
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/entry"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

// ovo je metoda koja će na startu sistema napuniti wal pool blokovima
// prolazimo kroz najskorašnjiji WAL fajl i učitavamo blokove u wal pool

func (bm *BlockManager) FillWalPool(walPath string) { // walFile je već kreiran samo ga prosledimo

	walFile := filepath.Base(walPath)

	// pročitamo iz konfiguracije system -> block_size i wal -> blocks_per_wal
	// i pomnožimo ih da dobijemo veličinu bloka
	// ======= UPOZORENE: ako je došlo do izmene u configu, moramo sve WAL-ove pre toga flushovati =======

	blockSize := config.ReadBlockSize()
	blocksPerWal := config.ReadBlocksPerWal()

	file, err := os.Open(walPath)
	HandleError(err, "Failed to open WAL file")
	defer file.Close()

	// prolazimo kroz wal fajl i učitavamo blokove u buffer pool
	// ako zafali za ceo blok popunimo ga nulama do kraja
	// kreiramo blokova koliko je blocksPerWal

	for i := uint32(0); i < blocksPerWal; i++ {
		data := make([]byte, blockSize)
		n, err := file.Read(data[:])
		if err != nil && err != io.EOF {
			HandleError(err, "Failed to read from WAL file")
		}

		if n < len(data) {
			for j := n; j < len(data); j++ {
				data[j] = 0
			}
		}

		bb := &BufferBlock{
			FileName:    walFile,
			BlockNumber: i,
			Data:        data,
		}

		bm.WalPool.AddBlock(bb)
	}
}

func (bm *BlockManager) GetBlockFromWalPool(index uint32) *BufferBlock {
	for e := bm.WalPool.Pool.Front(); e != nil; e = e.Next() {
		bb := e.Value.(*BufferBlock)
		if bb.BlockNumber == index {
			return bb
		}
	}
	return nil
}

func (bm *BlockManager) WriteWalPoolToWal(walPath string) string { // upisuje i pravi novi wal fajl
	file, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	HandleError(err, "Failed to open WAL file")
	defer file.Close()

	for e := bm.WalPool.Pool.Front(); e != nil; e = e.Next() {
		block := e.Value.(*BufferBlock)
		_, err := file.Write(block.Data)
		HandleError(err, "Failed to write block to WAL file")
	}

	err = file.Sync() // stable write
	HandleError(err, "Failed to sync WAL file")

	bm.WalPool.Clear()

	walFile := filepath.Base(walPath)

	num, _ := strconv.Atoi(walFile[4:])
	newWalFile := fmt.Sprintf("wal_%05d", num+1)

	_, err = os.Create(WalsPath + newWalFile)
	HandleError(err, "Failed to create new WAL file")

	bm.FillWalPool(WalsPath + newWalFile)

	return WalsPath + newWalFile
}

func (bm *BlockManager) SyncWalPoolToWal(walPath string) {
	file, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	HandleError(err, "Failed to open WAL file")
	defer file.Close()

	for e := bm.WalPool.Pool.Front(); e != nil; e = e.Next() {
		block := e.Value.(*BufferBlock)
		_, err := file.WriteAt(block.Data, int64(block.BlockNumber*uint32(len(block.Data))))
		HandleError(err, "Failed to write block to WAL file")
	}

	err = file.Sync() // stable write
	HandleError(err, "Failed to sync WAL file")
}

func (bm *BlockManager) GetEntriesFromSpecificWal(walName string) []entry.Entry {
	// dobija ime tipa "wal_00001"
	// napomena: neophodno je nakon promene broja blokova po walu flushovati sve walove
	// jer se ova metoda oslanja na config fajl

	walPath := WalsPath + walName
	blockSize := config.ReadBlockSize()

	file, err := os.Open(walPath)
	HandleError(err, "Failed to open WAL file")
	fileInfo, err := file.Stat()
	HandleError(err, "Failed to get file info")
	if fileInfo.Size()%int64(blockSize) != 0 {
		log.Fatalf("WAL file is corrupted")
	}

	entries := make([]entry.Entry, 0)
	partialEntries := make([][]byte, 0) // praznimo kada se složi entry TYPE = 4 (LAST)

	// učitamo sve iz wala u data
	walData := make([]byte, fileInfo.Size())

	// učitamo u walData sve podatke iz wala
	_, err = file.Read(walData)
	HandleError(err, "Failed to read WAL file")

	// iteriramo po blokovima
	for i := uint32(0); i < uint32(fileInfo.Size())/blockSize; i++ {
		positionInBlock := uint32(0)
		for positionInBlock < blockSize {

			if walData[i*blockSize+positionInBlock] == 0 {
				positionInBlock++
				continue
			}

			var entrySize uint32

			keySize := entry.BytesToUint64(walData[i*blockSize+positionInBlock+entry.KEY_SIZE_START : i*blockSize+positionInBlock+entry.VALUE_SIZE_START])
			valueSize := entry.BytesToUint64(walData[i*blockSize+positionInBlock+entry.VALUE_SIZE_START : i*blockSize+positionInBlock+entry.KEY_START])
			typeByte := walData[i*blockSize+positionInBlock+entry.TYPE_START]

			entrySize = uint32(entry.CRC_SIZE + entry.TIMESTAMP_SIZE + entry.TOMBSTONE_SIZE + entry.TYPE_SIZE + entry.KEY_SIZE_SIZE + entry.VALUE_SIZE_SIZE + keySize + valueSize)

			// ako je veličina entrija veća od preostalog dela bloka, grabi samo koliko možeš do kraja bloka
			if positionInBlock+entrySize > blockSize {
				entrySize = blockSize - positionInBlock
			}

			entryData := walData[i*blockSize+positionInBlock : i*blockSize+positionInBlock+entrySize]

			if typeByte == 1 {
				entries = append(entries, entry.ConstructEntry(entryData))
			} else {
				partialEntries = append(partialEntries, entryData)
			}

			positionInBlock += entrySize

			if typeByte == 4 {
				// složimo entry
				entries = append(entries, ConstructEntryFromPartialEntries(partialEntries))
				partialEntries = make([][]byte, 0)
			}
		}
	}

	return entries
}

func (bm *BlockManager) GetEntriesFromLeftoverWals() []entry.Entry {
	// napomena: neophodno je nakon promene broja blokova po walu flushovati sve walove
	// jer se ova metoda oslanja na config fajl

	files, err := os.ReadDir(WalsPath)
	HandleError(err, "Failed to read WALS folder")

	for i := 0; i < len(files); i++ { // sortiranje po imenu
		for j := i + 1; j < len(files); j++ {
			if files[i].Name() > files[j].Name() {
				files[i], files[j] = files[j], files[i]
			}
		}
	}

	entries := make([]entry.Entry, 0)

	for _, file := range files {

		walPath := file.Name()

		walPath = WalsPath + walPath
		blockSize := config.ReadBlockSize()

		file, err := os.Open(walPath)
		HandleError(err, "Failed to open WAL file")
		fileInfo, err := file.Stat()
		HandleError(err, "Failed to get file info")
		if fileInfo.Size()%int64(blockSize) != 0 {
			log.Fatalf("WAL file is corrupted")
		}

		partialEntries := make([][]byte, 0) // praznimo kada se složi entry TYPE = 4 (LAST)

		// učitamo sve iz wala u data
		walData := make([]byte, fileInfo.Size())

		// učitamo u walData sve podatke iz wala
		_, err = file.Read(walData)
		HandleError(err, "Failed to read WAL file")

		// iteriramo po blokovima
		for i := uint32(0); i < uint32(fileInfo.Size())/blockSize; i++ {
			positionInBlock := uint32(0)
			for positionInBlock < blockSize {
				if walData[i*blockSize+positionInBlock] == 0 {
					positionInBlock++
					continue
				}

				var entrySize uint32

				keySize := entry.BytesToUint64(walData[i*blockSize+positionInBlock+entry.KEY_SIZE_START : i*blockSize+positionInBlock+entry.VALUE_SIZE_START])
				valueSize := entry.BytesToUint64(walData[i*blockSize+positionInBlock+entry.VALUE_SIZE_START : i*blockSize+positionInBlock+entry.KEY_START])
				typeByte := walData[i*blockSize+positionInBlock+entry.TYPE_START]

				entrySize = uint32(entry.CRC_SIZE + entry.TIMESTAMP_SIZE + entry.TOMBSTONE_SIZE + entry.TYPE_SIZE + entry.KEY_SIZE_SIZE + entry.VALUE_SIZE_SIZE + keySize + valueSize)

				// ako je veličina entrija veća od preostalog dela bloka, grabi samo koliko možeš do kraja bloka
				if positionInBlock+entrySize > blockSize {
					entrySize = blockSize - positionInBlock
				}

				entryData := walData[i*blockSize+positionInBlock : i*blockSize+positionInBlock+entrySize]

				if typeByte == 1 {
					entries = append(entries, entry.ConstructEntry(entryData))
				} else {
					partialEntries = append(partialEntries, entryData)
				}

				positionInBlock += entrySize

				if typeByte == 4 {
					// složimo entry
					entries = append(entries, ConstructEntryFromPartialEntries(partialEntries))
					partialEntries = make([][]byte, 0)
				}
			}
		}
	}

	return entries
}

// metoda koja će složiti entry na osnovu niza parcijalnih entrija, prima listu nizova bajtova
func ConstructEntryFromPartialEntries(partialEntries [][]byte) entry.Entry {
	// pročitamo veličinu ključa i vrednosti
	// svaki niz bajtova na početku ima header

	keySize := entry.BytesToUint64(partialEntries[0][entry.KEY_SIZE_START:entry.VALUE_SIZE_START])
	valueSize := entry.BytesToUint64(partialEntries[0][entry.VALUE_SIZE_START:entry.KEY_START])

	// izračunamo pre uklanjanja headera crc, timestamp, tombstone, type...
	timestamp := entry.BytesToUint64(partialEntries[0][entry.TIMESTAMP_START:entry.TOMBSTONE_START])
	tombstone := partialEntries[0][entry.TOMBSTONE_START:entry.TYPE_START]

	// uklonimo sa početka svakog niza bajtova header koji je crc, timestamp, tombstone, type, keySize, valueSize
	for i := 0; i < len(partialEntries); i++ {
		partialEntries[i] = partialEntries[i][entry.KEY_START:]
	}

	// spojimo sve entrije u jedan niz bajtova
	data := make([]byte, 0)
	for i := 0; i < len(partialEntries); i++ {
		data = append(data, partialEntries[i]...)
	}

	// sastavimo ključ i vrednost
	key := string(data[:keySize])
	value := data[keySize : keySize+valueSize]

	// izračunamo crc32
	crc32 := entry.CRC32(append([]byte(key), value...))

	return entry.Entry{
		CRC:       crc32,
		Timestamp: timestamp,
		Tombstone: tombstone[0],
		Type:      1, // nakon sastavljanja entry-a tip je uvek 1 (FULL)
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}
}
