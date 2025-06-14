package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/encoded_entry"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
	"NASP-NoSQL-Engine/internal/sstable"
	"NASP-NoSQL-Engine/internal/wal"
	"bytes"
	"encoding/binary"
	"log"
	"time"
)

const (
	SSTablesPath = "../data/sstables/"
)

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

const ( // kopija podataka iz entry.go
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	TYPE_SIZE       = 1 // FULL : 1, FIRST : 2, MIDDLE : 3, LAST : 4
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	TYPE_START       = TOMBSTONE_START + TOMBSTONE_SIZE
	KEY_SIZE_START   = TYPE_START + TYPE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

type WritePath struct {
	SSTableManager  *sstable.SSTableManager
	BlockManager    *block_manager.BlockManager
	WalManager      *wal.WalManager
	MemtableManager *memtable.MemtableManager
}

func NewWritePath(blockManager *block_manager.BlockManager, walManager *wal.WalManager, memtableManager *memtable.MemtableManager, sstableManager *sstable.SSTableManager) *WritePath {
	return &WritePath{
		SSTableManager:  sstableManager,
		BlockManager:    blockManager,
		WalManager:      walManager,
		MemtableManager: memtableManager,
	}
}

func (wpo *WritePath) WriteEntryToWal(key string, value string) uint32 {

	minimalRequiredSize := uint32(CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + TYPE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE)
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	blocksPerWal := wpo.WalManager.Wal.BlocksPerWAL // ispraviti kada se bude menjalo u configu
	blockSize := config.ReadBlockSize()             // ispraviti kada se bude menjalo u configu
	walSize := uint32(blockSize * blocksPerWal)

	if keySize+valueSize > (walSize - (blocksPerWal * minimalRequiredSize)) {
		return 3
	}

	walPoolCopy := block_manager.NewWalPool()

	for e := wpo.BlockManager.WalPool.Pool.Front(); e != nil; e = e.Next() {
		fileName := e.Value.(*block_manager.BufferBlock).FileName
		blockNumber := e.Value.(*block_manager.BufferBlock).BlockNumber
		data := make([]byte, len(e.Value.(*block_manager.BufferBlock).Data))
		copy(data, e.Value.(*block_manager.BufferBlock).Data)
		walPoolCopy.AddBlock(block_manager.NewBufferBlock(fileName, blockNumber, data, blockSize, false))
	}

	// nadjemo prvu slododnu poziciju u wal poolu koja je nula bajt 00000000
	// i to uzimamo kao početni položaj za upis

	positionInBlock := uint32(0)
	positionInWalPool := uint32(0)
	found := false

	for e := walPoolCopy.Pool.Back(); e != nil; e = e.Prev() {
		for i := int(blockSize) - 1; i >= 0; i-- {
			if e.Value.(*block_manager.BufferBlock).Data[i] != 0 {
				positionInBlock = uint32(i) + 1
				positionInWalPool = e.Value.(*block_manager.BufferBlock).BlockNumber
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	crc32 := uint32(entry.CRC32([]byte(key + value)))
	timestamp := uint64(time.Now().UnixNano())
	tombstone := byte(0)
	entryType := byte(1) // za početak nebitno jer je uvek full

	header := make([]byte, 0) // pravimo header koji će se uvek upisivati
	header = append(header, entry.Uint32ToBytes(crc32)...)
	header = append(header, entry.Uint64ToBytes(timestamp)...)
	header = append(header, tombstone)
	header = append(header, entryType)
	header = append(header, entry.Uint64ToBytes(uint64(keySize))...)
	header = append(header, entry.Uint64ToBytes(uint64(valueSize))...)

	compactValue := make([]byte, 0)
	compactValue = append(compactValue, []byte(key)...)
	compactValue = append(compactValue, []byte(value)...)

	typeArray := make([][2]uint32, 0) // pamti pozicije TYPE elemenata jer su one jedino bitne za kasniju izmenu
	failed := false                   // da li je upis iz prvog pokušaja uspeo

	tempPositionInBlock := positionInBlock
	tempPositionInWalPool := positionInWalPool

	compactBytesWritten := uint32(0)
	compactValueCurrentPosition := uint32(0)

	complete := false

	for e := walPoolCopy.Pool.Front(); e != nil; e = e.Next() {

		if complete {
			break
		}

		if e.Value.(*block_manager.BufferBlock).BlockNumber == tempPositionInWalPool {

			if tempPositionInBlock+uint32(len(header)) <= blockSize { // pokušamo da upišemo prvo header
				for i := 0; i < len(header); i++ {
					e.Value.(*block_manager.BufferBlock).Data[tempPositionInBlock] = header[i]
					tempPositionInBlock++

					if i == TYPE_START {
						typeArray = append(typeArray, [2]uint32{tempPositionInWalPool, tempPositionInBlock - 1})
					}
				}
			} else {
				if e.Next() == nil { // ako je blok poslednji u wal poolu, onda failed = true iskačemo iz svih petlji
					failed = true
					break
				} else {
					tempPositionInBlock = 0
					tempPositionInWalPool++
					continue
				}
			}

			for i := tempPositionInBlock; i < blockSize; i++ {
				e.Value.(*block_manager.BufferBlock).Data[i] = compactValue[compactValueCurrentPosition]
				compactValueCurrentPosition++
				compactBytesWritten++
				tempPositionInBlock++
				if compactBytesWritten == uint32(len(compactValue)) {
					complete = true
					break
				}
			}

			// ako je sledeći blok nil a nismo završili sa upisom, onda failed = true
			if e.Next() == nil && !complete {
				failed = true
				break
			}

			tempPositionInBlock = 0
			tempPositionInWalPool++

		} else {
			continue
		}
	}

	if failed {
		// znači da nam je wal pool već pun, treba upiasti sve iz poola

		newWalPath := wpo.BlockManager.WriteWalPoolToWal(wpo.WalManager.Wal.Path) // on već upisuje sve iz wal poola u wal, napravi nove fajlove i napuni buffer pool
		wpo.WalManager.Wal.Path = newWalPath

		// pozicije positionInBlock i positionInWalPool su već uradjene

		typeArray = make([][2]uint32, 0)
		compactBytesWritten = uint32(0)
		compactValueCurrentPosition = uint32(0)

		positionInBlock = uint32(0)
		positionInWalPool = uint32(0)

		complete = false

		for e := wpo.BlockManager.WalPool.Pool.Front(); e != nil; e = e.Next() {

			if complete {
				break
			}

			if e.Value.(*block_manager.BufferBlock).BlockNumber == positionInWalPool {

				if positionInBlock+uint32(len(header)) <= blockSize {
					for i := 0; i < len(header); i++ {
						e.Value.(*block_manager.BufferBlock).Data[positionInBlock] = header[i]
						positionInBlock++

						if i == TYPE_START {
							typeArray = append(typeArray, [2]uint32{positionInWalPool, positionInBlock - 1})
						}
					}
				} else {
					if e.Next() == nil {
						return 3
					} else {
						positionInBlock = 0
						positionInWalPool++
						continue
					}
				}

				for i := positionInBlock; i < blockSize; i++ {
					e.Value.(*block_manager.BufferBlock).Data[i] = compactValue[compactValueCurrentPosition]
					compactValueCurrentPosition++
					compactBytesWritten++
					positionInBlock++
					if compactBytesWritten == uint32(len(compactValue)) {
						complete = true
						break
					}
				}

				positionInBlock = 0
				positionInWalPool++

			} else {
				continue
			}
		}

		// sada na osnovu typeArray popunjavamo TYPE elemente

		if len(typeArray) == 1 {
			wpo.BlockManager.GetBlockFromWalPool(typeArray[0][0]).Data[typeArray[0][1]] = 1
		} else {
			wpo.BlockManager.GetBlockFromWalPool(typeArray[0][0]).Data[typeArray[0][1]] = 2
			wpo.BlockManager.GetBlockFromWalPool(typeArray[len(typeArray)-1][0]).Data[typeArray[len(typeArray)-1][1]] = 4
			for i := 1; i < len(typeArray)-1; i++ {
				wpo.BlockManager.GetBlockFromWalPool(typeArray[i][0]).Data[typeArray[i][1]] = 3
			}
		}
	} else {
		for e := walPoolCopy.Pool.Front(); e != nil; e = e.Next() {
			wpo.BlockManager.WalPool.AddBlock(block_manager.NewBufferBlock(e.Value.(*block_manager.BufferBlock).FileName,
				e.Value.(*block_manager.BufferBlock).BlockNumber, e.Value.(*block_manager.BufferBlock).Data, blockSize, false))
		}

		walPoolCopy = nil

		if len(typeArray) == 1 {
			wpo.BlockManager.GetBlockFromWalPool(typeArray[0][0]).Data[typeArray[0][1]] = 1
		} else {
			wpo.BlockManager.GetBlockFromWalPool(typeArray[0][0]).Data[typeArray[0][1]] = 2
			wpo.BlockManager.GetBlockFromWalPool(typeArray[len(typeArray)-1][0]).Data[typeArray[len(typeArray)-1][1]] = 4
			for i := 1; i < len(typeArray)-1; i++ {
				wpo.BlockManager.GetBlockFromWalPool(typeArray[i][0]).Data[typeArray[i][1]] = 3
			}
		}
	}

	// sinhronizacija wal poola sa wal fajlom
	wpo.BlockManager.SyncWalPoolToWal(wpo.WalManager.Wal.Path)
	return 0
}

func (wpo *WritePath) WriteEntriesToSSTable(entries *[]entry.Entry) uint32 {

	encodedEntries := make([]encoded_entry.EncodedEntry, 0)

	// ============================================================================================================ BITNA FUNKCIJA
	wpo.BlockManager.ReadBidirectionalMapFromFile()
	// ============================================================================================================ BITNA FUNKCIJA

	sst := wpo.SSTableManager.CreateSSTable() // sada biramo koji režim upisivanja u sstabelu radimo, merge ili standard
	compression := sst.Compression

	for _, e := range *entries {
		// ubacujemo ključeve u bidirekcioni map i uzimamo kompresovanu numeričku vrednost
		globalValue := wpo.BlockManager.BidirectionalMap.Add(e.Key)
		encodedEntries = append(encodedEntries, encoded_entry.EncodeEntry(e, globalValue, compression))
	}

	// ============================================================================================================ BITNA FUNKCIJA
	wpo.BlockManager.WriteBidirectionalMapToFile()
	// ============================================================================================================ BITNA FUNKCIJA

	// pravimo offsetes za index, odnosno listu od 3 para (key, position in block, block index) koja se kasnije prosledjuje createIndex metodi na obradu
	indexTuples := make([]sstable.IndexTuple, 0)

	merge := sst.Merge

	if merge {
		filePath := "..-data-sstables-" + sst.SSTableName + "-data"
		blockFileId := "sstables-" + sst.SSTableName + "-data"
		currentBlockIndex := uint32(0) // kako budem upisivali blokove, povećavaćemo ovaj broj (nije pravi index unutar data.bin)
		positionInBlock := uint32(4)   // počinje od 4 jer prih 8 byte-a ostavljamo za označavanje koji blok pripada kom delu (4 dodata + 4 od BF jer se ne koriste)
		wpo.BlockManager.BufferPool.AddBlock(block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false))
		currentBlock := wpo.BlockManager.BufferPool.GetBlock(blockFileId, currentBlockIndex)

		for _, e := range *entries {
			sst.BloomFilter.Add([]byte(e.Key))
		}

		var bfBuffer bytes.Buffer
		sst.BloomFilter.Serialize(&bfBuffer)
		bfData := bfBuffer.Bytes()
		for _, b := range bfData {
			for positionInBlock >= sst.BlockSize {
				currentBlock.WrittenStatus = true
				wpo.BlockManager.WriteBlock(filePath, currentBlock)
				currentBlockIndex++
				positionInBlock -= sst.BlockSize
				currentBlock = block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false)
			}

			currentBlock.Data[positionInBlock] = b
			positionInBlock++
		}

		// treba preći na sledeći blok nakon upisa bloom filtera
		wpo.BlockManager.WriteBlock(filePath, currentBlock)
		currentBlockIndex++
		positionInBlock = 0
		currentBlock = block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false)

		// upisuje se na kom bloku počinje data
		binary.BigEndian.PutUint16(wpo.BlockManager.BufferPool.GetBlock(blockFileId, 0).Data[0:2], uint16(currentBlockIndex))

		// iteriramo kroz sve enkodirane entrije i upisujemo ih u dataBlocks
		for _, e := range encodedEntries {
			compactValue := make([]byte, 0)
			compactValue = append(compactValue, e.Key...)
			compactValue = append(compactValue, e.Value...)

			indexTupleWritten := false // da li je index tuple upisan

			typeArray := make([][2]uint32, 0) // pamti pozicije TYPE elemenata

			compactBytesWritten := uint32(0)
			compactValueCurrentPosition := uint32(0)

			crcStart := uint32(0) // računamo start za svaki element jer se sada svaki put razlikuje u encoded entriju
			timestampStart := crcStart + uint32(len(e.CRC))
			tombstoneStart := timestampStart + uint32(len(e.Timestamp))
			typeStart := tombstoneStart + uint32(len(e.Tombstone))

			header := make([]byte, 0) // pravimo header koji će se uvek upisivati
			header = append(header, e.CRC...)
			header = append(header, e.Timestamp...)
			header = append(header, e.Tombstone...)
			header = append(header, e.Type...)
			header = append(header, e.KeySize...)
			header = append(header, e.ValueSize...)

			complete := false

			// izvršavamo dokle god ne bude complete, prave se novi blokovi i dodaju u buffer pool
			for !complete {
				// provera da li od trenutne pozicije u bloku ima dovoljno mesta za header
				if positionInBlock+uint32(len(header)) <= sst.BlockSize {

					if !indexTupleWritten {
						indexTuples = append(indexTuples, sstable.IndexTuple{Key: e.Key, PositionInBlock: positionInBlock, BlockIndex: currentBlockIndex})
						indexTupleWritten = true
					}

					for i := 0; i < len(header); i++ {
						currentBlock.Data[positionInBlock] = header[i]
						positionInBlock++

						if i == int(typeStart) {
							typeArray = append(typeArray, [2]uint32{currentBlockIndex, positionInBlock - 1})
						}
					}
				} else {
					sst.Metadata.AddBlock(&currentBlock.Data) // kada se blok napuni dodajemo ga u merkle stablo
					currentBlock.WrittenStatus = true
					wpo.BlockManager.WriteBlock(filePath, currentBlock)

					// povećavamo indeks i kreiramo novi blok
					currentBlockIndex++
					currentBlock = block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false)
					positionInBlock = 0
					continue
				}

				// upisujemo ključ i vrednost
				for i := positionInBlock; i < sst.BlockSize; i++ {
					currentBlock.Data[i] = compactValue[compactValueCurrentPosition]
					compactValueCurrentPosition++
					compactBytesWritten++
					positionInBlock++
					if compactBytesWritten == uint32(len(compactValue)) {
						complete = true
						break
					}
				}

				// ako smo završili sa upisom, onda postavljamo TYPE elemente
				if complete {
					if len(typeArray) == 1 {
						currentBlock.Data[typeArray[0][1]] = 1
					} else {
						currentBlock.Data[typeArray[len(typeArray)-1][1]] = 4
					}

				} else {
					if len(typeArray) == 1 {
						currentBlock.Data[typeArray[0][1]] = 2
					} else {
						currentBlock.Data[typeArray[len(typeArray)-1][1]] = 3
					}

					sst.Metadata.AddBlock(&currentBlock.Data)
					currentBlock.WrittenStatus = true
					wpo.BlockManager.WriteBlock(filePath, currentBlock)

					currentBlockIndex++
					currentBlock = block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false)
					positionInBlock = 0
				}
			}
		}

		// upisuje se poslednji blok ako nije već upisan (desi se ako nije skroz popunjen)
		if !currentBlock.WrittenStatus {
			sst.Metadata.AddBlock(&currentBlock.Data)
			wpo.BlockManager.WriteNONMergeBlock(currentBlock)
			currentBlockIndex++
		}

		// dobavljamo 1. blok da upišemo na kom bloku počinje index
		currentBlock = wpo.BlockManager.BufferPool.GetBlock(blockFileId, 0)
		binary.BigEndian.PutUint16(currentBlock.Data[2:4], uint16(currentBlockIndex))

		indexData := wpo.SSTableManager.CreateNONMergeIndex(indexTuples, sst.BlockSize)
		summaryData := wpo.SSTableManager.CreateNONMergeSummary(indexTuples, indexData, compression, currentBlockIndex*sst.BlockSize)

		wpo.BlockManager.WriteBytesAsBlocks(*indexData, filePath, currentBlockIndex)
		currentBlockIndex += (uint32(len(*indexData)) + sst.BlockSize - 1) / sst.BlockSize // odlaže se povećanje indeksa da bi summary offset bio tačan
		binary.BigEndian.PutUint16(currentBlock.Data[4:6], uint16(currentBlockIndex))      // upisuje se na kom bloku počinje summary

		wpo.BlockManager.WriteBytesAsBlocks(summaryData, filePath, currentBlockIndex)
		currentBlockIndex += (uint32(len(summaryData)) + sst.BlockSize - 1) / sst.BlockSize
		binary.BigEndian.PutUint16(currentBlock.Data[6:8], uint16(currentBlockIndex)) // upisuje se na kom bloku počinje merkle (metadata)

		sst.Metadata.Build() // nakon dodavanja svih data blokova radi se build za merkle stablo
		wpo.BlockManager.WriteBytesAsBlocks(*sst.Metadata.Serialize(), filePath, currentBlockIndex)

		// upisujemo 1. blok ponovo sa sada zabeleženim podacima o početku svake sekcije
		wpo.BlockManager.WriteBlock(filePath, currentBlock)
	} else {
		// dakle ideja je da se entriji upisuju u sstable po sličnom principu kao i u wal
		// samo što ove ne znamo koliko će blokova trebati i onda se oni usput kreiraju
		// svaki blok kada se napravi, upisuje se u sstable
		// usput i veličinu headera ne znamo unapred, pa ćemo je računati
		// otvaramo fajl na putanji sstable_xxxxxx

		currentBlockIndex := uint32(0) // kako budem upisivali blokove, povećavaćemo ovaj broj
		positionInBlock := uint32(0)   // pozicija u bloku, kada se popuni, prelazimo na sledeći blok
		wpo.BlockManager.BufferPool.AddBlock(block_manager.NewBufferBlock("sstables-"+sst.SSTableName+"-"+"data", currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false))

		// iteriramo kroz sve enkodirane entrije i upisujemo ih u buffer blokove
		for _, e := range encodedEntries {
			compactValue := make([]byte, 0)
			compactValue = append(compactValue, e.Key...)
			compactValue = append(compactValue, e.Value...)

			indexTupleWritten := false // da li je index tuple upisan

			typeArray := make([][2]uint32, 0) // pamti pozicije TYPE elemenata

			compactBytesWritten := uint32(0)
			compactValueCurrentPosition := uint32(0)

			crcStart := uint32(0) // računamo start za svaki element jer se sada svaki put razlikuje u encoded entriju
			timestampStart := crcStart + uint32(len(e.CRC))
			tombstoneStart := timestampStart + uint32(len(e.Timestamp))
			typeStart := tombstoneStart + uint32(len(e.Tombstone))

			header := make([]byte, 0) // pravimo header koji će se uvek upisivati
			header = append(header, e.CRC...)
			header = append(header, e.Timestamp...)
			header = append(header, e.Tombstone...)
			header = append(header, e.Type...)
			header = append(header, e.KeySize...)
			header = append(header, e.ValueSize...)

			complete := false

			// izvršavamo dokle god ne bude complete, prave se novi blokovi i dodaju u buffer pool
			for !complete {
				bufferBlock := wpo.BlockManager.BufferPool.GetBlock("sstables-"+sst.SSTableName+"-"+"data", currentBlockIndex) // mora ime fajla odgovarati

				// provera da li od trenutne pozicije u bloku ima dovoljno mesta za header
				if positionInBlock+uint32(len(header)) <= sst.BlockSize {

					if !indexTupleWritten {
						indexTuples = append(indexTuples, sstable.IndexTuple{Key: e.Key, PositionInBlock: positionInBlock, BlockIndex: currentBlockIndex})
						indexTupleWritten = true
					}

					for i := 0; i < len(header); i++ {
						bufferBlock.Data[positionInBlock] = header[i]
						positionInBlock++

						if i == int(typeStart) {
							typeArray = append(typeArray, [2]uint32{currentBlockIndex, positionInBlock - 1})
						}
					}
				} else {
					// pozovemo metodu koja upisuje blok u sstable i pravimo novi blok (NON-MERGE)
					block := wpo.BlockManager.BufferPool.GetBlock("sstables-"+sst.SSTableName+"-"+"data", currentBlockIndex)

					if !block.WrittenStatus { // ako blok nije upisan, upisujemo ga
						wpo.BlockManager.WriteNONMergeBlock(block)
					}

					// povećavamo indeks i idemo dalje
					currentBlockIndex++
					wpo.BlockManager.BufferPool.AddBlock(block_manager.NewBufferBlock("sstables-"+sst.SSTableName+"-"+"data", currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false))
					positionInBlock = 0
					continue
				}

				// upisujemo ključ i vrednost
				for i := positionInBlock; i < sst.BlockSize; i++ {
					bufferBlock.Data[i] = compactValue[compactValueCurrentPosition]
					compactValueCurrentPosition++
					compactBytesWritten++
					positionInBlock++
					if compactBytesWritten == uint32(len(compactValue)) {
						complete = true
						break
					}
				}

				// ako smo završili sa upisom, onda postavljamo TYPE elemente
				if complete {
					if len(typeArray) == 1 {
						wpo.BlockManager.BufferPool.GetBlock("sstables-"+sst.SSTableName+"-"+"data", typeArray[0][0]).Data[typeArray[0][1]] = 1
					} else {
						block := wpo.BlockManager.BufferPool.GetBlock("sstables-"+sst.SSTableName+"-"+"data", typeArray[0][0])
						block.Data[typeArray[0][1]] = 2 // miljan fixx
						wpo.BlockManager.WriteNONMergeBlock(block)
						wpo.BlockManager.BufferPool.GetBlock("sstables-"+sst.SSTableName+"-"+"data", typeArray[len(typeArray)-1][0]).Data[typeArray[len(typeArray)-1][1]] = 4
						for i := 1; i < len(typeArray)-1; i++ {
							block := wpo.BlockManager.BufferPool.GetBlock("sstables-"+sst.SSTableName+"-"+"data", typeArray[i][0])
							block.Data[typeArray[i][1]] = 3 // miljan fixx
							wpo.BlockManager.WriteNONMergeBlock(block)
						}
					}

				} else {
					block := wpo.BlockManager.BufferPool.GetBlock("sstables-"+sst.SSTableName+"-"+"data", currentBlockIndex)

					if !block.WrittenStatus {
						wpo.BlockManager.WriteNONMergeBlock(block)
					}

					currentBlockIndex++
					wpo.BlockManager.BufferPool.AddBlock(block_manager.NewBufferBlock("sstables-"+sst.SSTableName+"-"+"data", currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false))
					positionInBlock = 0
				}
			}
		}

		// kada smo završili sa upisom svih entrija, potrebno je upisati poslednji blok u sstable
		// ovo treba proveriti zato što je moguće da će se upisati MOŽDA 2 puta poslednji blok...                         Bogdan

		block := wpo.BlockManager.BufferPool.GetBlock("sstables-"+sst.SSTableName+"-"+"data", currentBlockIndex)

		if !block.WrittenStatus {
			wpo.BlockManager.WriteNONMergeBlock(block)
		}

		// kreiramo index i upisujemo ga u sstable
		index := wpo.SSTableManager.CreateNONMergeIndex(indexTuples, sst.BlockSize) // znak pitanja da li treba da se pravi po blokovima ili sve odjednom...
		wpo.BlockManager.WriteNONMergeIndex(*index, sst.SSTableName)

		// sada kreiramo summary
		summary := wpo.SSTableManager.CreateNONMergeSummary(indexTuples, index, compression, 0)
		wpo.BlockManager.WriteNONMergeSummary(summary, sst.SSTableName)

		// potrebno je dodati elemente u bloom filter koji je već kreiran, samo ubacimo ključeve
		for _, e := range *entries {
			sst.BloomFilter.Add([]byte(e.Key))
		}

		// serijalizujemo bloom filter i upisujemo ga u sstable
		wpo.BlockManager.WriteNONMergeBloomFilter(sst.BloomFilter, sst.SSTableName)

		// sada je potrebno napraviti metadata (odnosno merkle stablo) i upisati ga u sstable
		metadata := wpo.BlockManager.CreateAndWriteNONMergeMetadata(SSTablesPath+sst.SSTableName+"/"+sst.DataName,
			SSTablesPath+sst.SSTableName+"/"+sst.MetadataName)
		sst.Metadata = metadata
	}

	// dodajemo sstable u listu svih sstabela
	wpo.SSTableManager.AddSSTable(sst)

	return 0
}
