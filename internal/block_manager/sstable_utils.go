package block_manager

import (
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
)

func (bm *BlockManager) CreateMergeFiles(sstableName string) {
	_, err := os.Create(SSTablesPath + sstableName + "/data")
	HandleError(err, "Failed to create data file")
	_, err = os.Create(SSTablesPath + sstableName + "/blocksize")
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(SSTablesPath + sstableName + "/merge")
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(SSTablesPath + sstableName + "/toc")
	HandleError(err, "Failed to create toc file")
	_, err = os.Create(SSTablesPath + sstableName + "/compression")
	HandleError(err, "Failed to create compression file")
}

func (bm *BlockManager) CreateStandardFiles(sstableName string) {
	_, err := os.Create(SSTablesPath + sstableName + "/data")
	HandleError(err, "Failed to create data file")
	_, err = os.Create(SSTablesPath + sstableName + "/index")
	HandleError(err, "Failed to create index file")
	_, err = os.Create(SSTablesPath + sstableName + "/summary")
	HandleError(err, "Failed to create summary file")
	_, err = os.Create(SSTablesPath + sstableName + "/metadata")
	HandleError(err, "Failed to create metadata file")
	_, err = os.Create(SSTablesPath + sstableName + "/bloomfilter")
	HandleError(err, "Failed to create bloomfilter file")
	_, err = os.Create(SSTablesPath + sstableName + "/blocksize")
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(SSTablesPath + sstableName + "/merge")
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(SSTablesPath + sstableName + "/toc")
	HandleError(err, "Failed to create toc file")
	_, err = os.Create(SSTablesPath + sstableName + "/compression")
	HandleError(err, "Failed to create compression file")
}

// funkcija koja čita block size iz fajla na osnovu putanje
func (bm *BlockManager) ReadBlockSize(path string) uint32 {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	HandleError(err, "Failed to open block size file")

	var blockSize uint32
	err = binary.Read(file, binary.BigEndian, &blockSize)
	HandleError(err, "Failed to read block size from file")

	return blockSize
}

func (bm *BlockManager) WriteBlockSize(path string, blockSize uint32) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open blocksize file")
	err = binary.Write(file, binary.BigEndian, blockSize)
	HandleError(err, "Failed to write block size to blocksize file")
	file.Close()
}

func (bm *BlockManager) WriteNONMergeTOC(path string) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open toc file")
	tocData := []string{"data", "index", "summary", "metadata", "bloomfilter", "blocksize", "merge", "compression"}
	for _, entry := range tocData {
		err = binary.Write(file, binary.BigEndian, uint32(len(entry)))
		HandleError(err, "Failed to write toc entry length to toc file")
		_, err = file.Write([]byte(entry))
		HandleError(err, "Failed to write toc entry to toc file")
	}
	file.Close()
}

func (bm *BlockManager) WriteMerge(path string, merge bool) { // 1 ako je merge, 0 ako nije
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open merge file")
	mergeByte := byte(0)
	if merge {
		mergeByte = byte(1)
	}
	_, err = file.Write([]byte{mergeByte})
	HandleError(err, "Failed to write merge to merge file")
	file.Close()
}

func (bm *BlockManager) WriteCompression(path string, compression bool) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open compression file")
	compressionByte := byte(0)
	if compression {
		compressionByte = byte(1)
	}
	_, err = file.Write([]byte{compressionByte})
	HandleError(err, "Failed to write compression to compression file")
	file.Close()
}

func (bm *BlockManager) WriteMergeTOC(path string) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open toc file")
	tocData := []string{"data", "blocksize", "merge", "compression"}
	for _, entry := range tocData {
		err = binary.Write(file, binary.BigEndian, uint32(len(entry)))
		HandleError(err, "Failed to write toc entry length to toc file")
		_, err = file.Write([]byte(entry))
		HandleError(err, "Failed to write toc entry to toc file")
	}
	file.Close()
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
