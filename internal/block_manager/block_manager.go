package block_manager

import (
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"encoding/binary"
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
	entrySize := uint32(entry.CRC_SIZE + entry.TIMESTAMP_SIZE + entry.TOMBSTONE_SIZE + entry.TYPE_SIZE + entry.KEY_SIZE_SIZE + entry.VALUE_SIZE_SIZE + keySize + valueSize)

	crc32 := uint32(entry.CRC32(partialEntries[0][entry.CRC_START : entry.TIMESTAMP_START+entrySize]))
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

// funkcija koja upisuje u fajl non-merge index strukturu
// PADDING je NA ZADNJEM BLOKU
func (bm *BlockManager) WriteNONMergeIndex(index []byte, sstable string) {

	blockSize := config.ReadBlockSize()

	filePath := SSTablesPath + sstable + "/" + "index"

	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	HandleError(err, "Failed to open file")
	defer f.Close()

	totalSize := uint32(len(index))
	numBlocks := (totalSize + blockSize - 1) / blockSize // plafoniranje

	for i := uint32(0); i < numBlocks; i++ {
		start := i * blockSize
		end := start + blockSize
		if end > totalSize {
			end = totalSize
		}

		block := make([]byte, blockSize)
		copy(block, index[start:end])

		// kreiramo blok i upisujemo ga u fajl pomoću write block metode
		bb := &BufferBlock{
			FileName:    "sstables-" + sstable + "-index",
			BlockNumber: i,
			Data:        block,
			BlockSize:   blockSize,

			WrittenStatus: true,
		}

		bm.WriteBlock(filePath, bb)
		bm.BufferPool.AddBlock(bb)
	}

	err = f.Sync()
	HandleError(err, "Failed to sync file")
}

// funkcija koja upisuje non-merge summary u fajl blokovski
// PADDING je na ZADNJEM BLOKU
func (bm *BlockManager) WriteNONMergeSummary(summary []byte, sstable string) {

	blockSize := config.ReadBlockSize()

	filePath := SSTablesPath + sstable + "/" + "summary"

	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	HandleError(err, "Failed to open file")
	defer f.Close()

	totalSize := uint32(len(summary))
	numBlocks := (totalSize + blockSize - 1) / blockSize // plafoniranje

	for i := uint32(0); i < numBlocks; i++ {
		start := i * blockSize
		end := start + blockSize
		if end > totalSize {
			end = totalSize
		}

		block := make([]byte, blockSize)
		copy(block, summary[start:end])

		// kreiramo novi blok i dodajemo u buffer pool
		bb := &BufferBlock{
			FileName:    "sstables-" + sstable + "-summary",
			BlockNumber: i,
			Data:        block,
			BlockSize:   blockSize,

			WrittenStatus: true,
		}

		bm.WriteBlock(filePath, bb)
		bm.BufferPool.AddBlock(bb) // OBAVEZNO NA KRAJU DODAMO U BUFFER POOL!!!!!!!!
	}

	err = f.Sync()
	HandleError(err, "Failed to sync file")
}

// non merge funkcija koja upisuje bloom filter u fajl blokovski
// NAPOMENA: ovde se koristi bytes.Buffer
// NAPOMENA: blokovski upis ne pravi padding na zadnjeg bloka kasnije zbog deserijalizacije
func (bm *BlockManager) WriteNONMergeBloomFilter(bf *probabilistics.BloomFilter, sstable string) {
	filePath := SSTablesPath + sstable + "/" + "bloomfilter"

	var buffer bytes.Buffer
	if err := bf.Serialize(&buffer); err != nil {
		log.Fatalf("Failed to serialize bloom filter: %v", err)
	}

	blockSize := config.ReadBlockSize()
	data := buffer.Bytes()
	totalSize := uint32(len(data))
	numBlocks := (totalSize + blockSize - 1) / blockSize

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	HandleError(err, "Failed to open file")
	defer file.Close()

	for i := uint32(0); i < numBlocks; i++ {
		start := i * blockSize
		end := start + blockSize
		if end > totalSize {
			end = totalSize
		}

		var block []byte
		if end-start < blockSize {
			block = data[start:end] // Tačna veličina poslednjeg bloka
		} else {
			block = make([]byte, blockSize)
			copy(block, data[start:end])
		}

		bb := &BufferBlock{
			FileName:      "sstables-" + sstable + "-bloomfilter",
			BlockNumber:   i,
			Data:          block,
			BlockSize:     uint32(len(block)), // sada imamo preciznu veličinu bloka
			WrittenStatus: true,
		}

		bm.WriteBlock(filePath, bb)
		bm.BufferPool.AddBlock(bb)
	}

	err = file.Sync()
	HandleError(err, "Failed to sync file")
}


// funkcija koja kreira Metadata (merkle stablo)
// prolazi blokovski kroz data fajl i samo dodaje blokove
// kasnije se stablo bilduje i serijalizuje
// PADDING NIJE POTREBAN TAKODJE
func (bm *BlockManager) CreateAndWriteNONMergeMetadata(dataPath string, metadataPath string) *trees.MerkleTree {

	mt := trees.NewMerkleTree()

	file, err := os.Open(dataPath)
	HandleError(err, "Failed to open file")

	blockSize := config.ReadBlockSize()

	for {
		data := make([]byte, blockSize)
		n, err := file.Read(data)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to read from file: %v", err)
		}

		if n < len(data) {
			break
		}

		mt.AddBlock(&data)
	}

	// bildujemo stablo
	mt.Build()

	err = file.Close()
	HandleError(err, "Failed to close file")

	// serijalizujemo merkle
	merkleBytes := mt.Serialize()

	// upisujemo u fajl blokovski bajtove
	file, err = os.OpenFile(metadataPath, os.O_RDWR|os.O_CREATE, 0666)
	HandleError(err, "Failed to open file")
	defer file.Close()

	totalSize := uint32(len(*merkleBytes))
	numBlocks := (totalSize + blockSize - 1) / blockSize // plafoniranje

	for i := uint32(0); i < numBlocks; i++ {
		start := i * blockSize
		end := start + blockSize
		if end > totalSize {
			end = totalSize
		}

		block := make([]byte, blockSize)
		copy(block, (*merkleBytes)[start:end])

		// kreiramo novi blok i dodajemo u buffer pool
		bb := &BufferBlock{
			FileName:    "sstables-" + metadataPath + "-metadata",
			BlockNumber: i,
			Data:        block,
			BlockSize:   blockSize,
			
			WrittenStatus: true,
		}

		bm.WriteBlock(metadataPath, bb)
		bm.BufferPool.AddBlock(bb) // OBAVEZNO NA KRAJU DODAMO U BUFFER POOL!!!!!!!!
	}

	err = file.Sync()
	HandleError(err, "Failed to sync file")

	return mt
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

	fmt.Println("CRCList: ", bm.CRCList)
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
				fmt.Println("Removed CRC: ", entry.CRC)
			}

			// printujemo kako trenutno izgleda CRCList
			fmt.Println("CRCList: ", bm.CRCList)

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
