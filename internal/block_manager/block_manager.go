package block_manager

import (
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/entry"
	"encoding/binary"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	WalsPath        = "../data/wals/"
	ConfigPath      = "../data/config.json"
	SSTablesPath    = "../data/sstables/"
	FlushedCRCsPath = "../data/flushed_crcs"
)

type BlockManager struct {
	BufferPool *BufferPool
	WalPool    *WalPool
	CachePool  *CachePool

	CRCList []uint32 // neophodno za RemoveExpiredWals
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func NewBlockManager() *BlockManager {
	return &BlockManager{

		BufferPool: NewBufferPool(),
		WalPool:    NewWalPool(),
		CachePool:  NewCachePool(),
		CRCList:    make([]uint32, 0),
	}
}

func (bm *BlockManager) WriteBlock(path string, block *BufferBlock) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	HandleError(err, "Failed to open file")
	defer file.Close()

	_, err = file.WriteAt(block.Data, int64(block.BlockNumber*uint32(len(block.Data))))
	HandleError(err, "Failed to write block to file")

	err = file.Sync()
	HandleError(err, "Failed to sync file")

	// menjamo WrittenStatus na true
	block.WrittenStatus = true
}

// pravimo funkciju koja će na da pročita sadržaj bloka na odredjenoj putanji
func (bm *BlockManager) ReadBlock(path string, blockNumber uint32, blockSize uint32) *BufferBlock {

	file, err := os.Open(path)
	HandleError(err, "Failed to open file")
	defer file.Close()

	data := make([]byte, blockSize)
	// čitamo onoliko koliko možemo, ako je manje od bloka, popunjavamo nulama
	n, err := file.ReadAt(data, int64(blockNumber*blockSize))
	if err != nil && err != io.EOF {
		log.Fatalf("Failed to read from file: %v", err)
	}

	// binarne nule dodajemo
	if n < len(data) {
		for i := n; i < len(data); i++ {
			data[i] = 0
		}
	}

	// kreiramo blok i vraćamo ga, gde je filename:
	// npr. ../data/sstables/sstable_00001/data -> sstables-sstable_00001-data
	// npr. ../data/wals/wal_00001 -> wals-wal_00001
	// splitujemo po / i uzimamo sve delove nakon data i spajamo ih

	s := strings.Split(path, "/")
	pos := 0
	for i := 0; i < len(s); i++ {
		if s[i] == "data" {
			pos = i
			break
		}
	}

	fileName := strings.Join(s[pos+1:], "-")

	return &BufferBlock{
		FileName:    fileName,
		BlockNumber: blockNumber,
		Data:        data,
	}
}

// prihvata buffer block
func (bm *BlockManager) WriteNONMergeBlock(block *BufferBlock) {

	fileName := block.FileName

	// pravimo putanju do fajla
	// filename može biti sstables-sstable_00001-data ili wals-wal_00001
	// splitujemo po - i uzimamo sve delove nakon sstables ili wals i spajamo ih
	s := strings.Split(fileName, "-")
	pos := 0
	for i := 0; i < len(s); i++ {
		if s[i] == "sstables" || s[i] == "wals" {
			pos = i
			break
		}
	}

	filePath := SSTablesPath + strings.Join(s[pos+1:], "/")

	// koristimo write block metodu
	bm.WriteBlock(filePath, block)
	block.WrittenStatus = true
	bm.BufferPool.AddBlock(block) // OBAVEZNO NA KRAJU DODAMO U BUFFER POOL!!!!!!!!
}

// funkcija koja samo splituje string po - i vraća 2 dela
func SplitFileName(id string) (string, string) {
	s := strings.Split(id, "-")
	return s[0], s[1]
}

// funkcija koja čita flushed crcs iz fajla, crcovi su popakovani jedan iza drugog binarno
// BigEndian, uint32 (4 bajta)
func (bm *BlockManager) ReadFlushedCRCs() {
	// pročitamo sve i splitujemo po 0 bajtu
	// svaki crc je 4 bajta
	// BigEndian

	// ako fajl ne postoji, pravimo novi
	if _, err := os.Stat(FlushedCRCsPath); os.IsNotExist(err) {
		_, err := os.Create(FlushedCRCsPath)
		HandleError(err, "Failed to create file")
	}

	file, err := os.Open(FlushedCRCsPath)
	HandleError(err, "Failed to open file")
	defer file.Close()

	// praznimo CRCList
	bm.CRCList = make([]uint32, 0)

	data := make([]byte, 4)
	for {
		_, err = file.Read(data)
		if err == io.EOF {
			break
		}
		HandleError(err, "Failed to read from file")

		crc := binary.BigEndian.Uint32(data)
		bm.CRCList = append(bm.CRCList, crc)

		_, err = file.Seek(1, io.SeekCurrent)
		HandleError(err, "Failed to seek in file")
	}
}

// funkcija koja upisuje flushed crcs u fajl
func (bm *BlockManager) WriteFlushedCRCs() {
	// ideja je da upijemo sve crc-ove iz CRCList u fajl
	// tako da imamo CRC pa 0 bajt, pa CRC pa 0 bajt...
	// BigEndian, uint32 (4 bajta)

	// kompletno brišemo fajl na putanji i kreiramo novi
	file, err := os.Create(FlushedCRCsPath)
	HandleError(err, "Failed to create file")
	defer file.Close()

	for _, crc := range bm.CRCList {
		err = binary.Write(file, binary.BigEndian, crc)
		HandleError(err, "Failed to write CRC to file")

		// upisujemo 0 bajt
		_, err = file.Write([]byte{0})
		HandleError(err, "Failed to write 0 byte to file")

		err = file.Sync()
		HandleError(err, "Failed to sync file")
	}
}

// funkcija koja će da čisti zastarele wal fajlove
func (bm *BlockManager) DetectExpiredWals() {
	// ideja je dakle napunit CRCList svim CRC entrijima koji su flushovani u sstable
	// i onda prolazimo kroz sve wal fajlove i njihove entrije
	// ako se desi da ni jedan CRC iz WAL fajla nije u CRCList, brišemo taj WAL fajl
	// odnosno postavljamo LowWatermark na broj trenutnog WAL fajla (npr. wal_00005 -> LowWatermark = 5)
	// ako je wal poslednji, najnoviji ili jedini, ne brišemo ga

	// pročitamo sve CRC-ove iz fajla
	bm.ReadFlushedCRCs()

	files, err := os.ReadDir(WalsPath)
	HandleError(err, "Failed to read WALS folder")

	// sortiranje po imenu rastuće (npr. wal_00001, wal_00002, wal_00003...)
	for i := 0; i < len(files); i++ {
		for j := i + 1; j < len(files); j++ {
			if files[i].Name() > files[j].Name() {
				files[i], files[j] = files[j], files[i]
			}
		}
	}

	for _, file := range files {
		walName := file.Name()

		entries := bm.GetEntriesFromSpecificWal(walName)

		// prolazimo kroz sve entrije u entries i proveravamo CRC
		// pošto je CRCList lista flushovanih CRC-ova, ako se svaki CRC iz entries nalazi u CRCList, brišemo WAL (postavljamo LowWatermark samo)
		// ako se bar jedan CRC ne nalazi u CRCList, ne brišemo WAL

		deleteWal := true
		for _, entry := range entries {
			if !bm.ContainsCRC(entry.CRC) {
				deleteWal = false
				break
			}
		}

		if deleteWal {
			// proveravamo da li je wal poslednji, ako jeste ne brišemo ga
			if walName == files[len(files)-1].Name() {
				break
			}

			// upisujemo novi LowWatermark u config.json
			// recimo wal_00015 -> LowWatermark = 15
			num, _ := strconv.Atoi(walName[4:])
			config.WriteLowWatermark(uint32(num))

			// brišemo CRC-ove iz CRCList za taj wal
			for _, entry := range entries {
				bm.RemoveCRC(entry.CRC)
			}

			// upisujemo CRCList u fajl
			bm.WriteFlushedCRCs()
		}
	}

	// upisujemo CRCList u fajl
	bm.WriteFlushedCRCs()
}

// funkcija koja proverava da li se CRC nalazi u CRCList
func (bm *BlockManager) ContainsCRC(crc uint32) bool {
	for _, c := range bm.CRCList {
		if c == crc {
			return true
		}
	}
	return false
}

// funkcija koja uklanja CRC iz CRCList
func (bm *BlockManager) RemoveCRC(crc uint32) {
	for i, c := range bm.CRCList {
		if c == crc {
			bm.CRCList = append(bm.CRCList[:i], bm.CRCList[i+1:]...)
			return
		}
	}
}

// funkcija koja dodaje entrije u CRCList (dobijamo listu crc-ova koji su flushovani)
// ulazna vrednost je lista entrija
func (bm *BlockManager) AddCRCsToCRCList(entries []entry.Entry) {
	for _, entry := range entries {
		if !bm.ContainsCRC(entry.CRC) {
			bm.CRCList = append(bm.CRCList, entry.CRC)
		}
	}
}
