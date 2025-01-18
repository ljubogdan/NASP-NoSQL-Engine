package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
	"NASP-NoSQL-Engine/internal/sstable"
	"NASP-NoSQL-Engine/internal/wal"
	"fmt"
	"log"
	"time"
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
	BlockManager    *block_manager.BlockManager
	WalManager      *wal.WalManager
	MemtableManager *memtable.MemtableManager
}

func NewWritePath(blockManager *block_manager.BlockManager, walManager *wal.WalManager, memtableManager *memtable.MemtableManager) *WritePath {
	return &WritePath{
		BlockManager:    blockManager,
		WalManager:      walManager,
		MemtableManager: memtableManager,
	}
}

func (wpo *WritePath) WriteEntryToWal(key string, value string) uint32 {

	minimalRequiredSize := uint32(CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + TYPE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE)
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	blocksPerWal := wpo.WalManager.Wal.BlocksPerWAL                              // ispraviti kada se bude menjalo u configu
	blockSize := uint32(len(wpo.BlockManager.GetBlockFromBufferPool(0).Data))
	walSize := uint32(blockSize * blocksPerWal)

	if keySize+valueSize > (walSize - (blocksPerWal * minimalRequiredSize)) {
		return 3
	}

	bufferPoolCopy := block_manager.NewBufferPool()

	for e := wpo.BlockManager.BufferPool.Pool.Front(); e != nil; e = e.Next() {
		fileName := e.Value.(*block_manager.BufferBlock).FileName
		blockNumber := e.Value.(*block_manager.BufferBlock).BlockNumber
		data := make([]byte, len(e.Value.(*block_manager.BufferBlock).Data))
		copy(data, e.Value.(*block_manager.BufferBlock).Data)
		bufferPoolCopy.AddBlock(block_manager.NewBufferBlock(fileName, blockNumber, data))
	}

	// nadjemo prvu slododnu poziciju u buffer poolu koja je nula bajt 00000000
	// i to uzimamo kao početni položaj za upis

	positionInBlock := uint32(0)
	positionInBufferPool := uint32(0)
	found := false

	for e := bufferPoolCopy.Pool.Back(); e != nil; e = e.Prev() {
		for i := int(blockSize) - 1; i >= 0; i-- {
			if e.Value.(*block_manager.BufferBlock).Data[i] != 0 {
				positionInBlock = uint32(i) + 1
				positionInBufferPool = e.Value.(*block_manager.BufferBlock).BlockNumber
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
	tempPositionInBufferPool := positionInBufferPool

	compactBytesWritten := uint32(0)
	compactValueCurrentPosition := uint32(0)

	complete := false

	for e := bufferPoolCopy.Pool.Front(); e != nil; e = e.Next() {

		if complete {
			break
		}

		if e.Value.(*block_manager.BufferBlock).BlockNumber == tempPositionInBufferPool {

			if tempPositionInBlock+uint32(len(header)) <= blockSize { // pokušamo da upišemo prvo header
				for i := 0; i < len(header); i++ {
					e.Value.(*block_manager.BufferBlock).Data[tempPositionInBlock] = header[i]
					tempPositionInBlock++

					if i == TYPE_START {
						typeArray = append(typeArray, [2]uint32{tempPositionInBufferPool, tempPositionInBlock - 1})
					}
				}
			} else {
				if e.Next() == nil { // ako je blok poslednji u buffer poolu, onda failed = true iskačemo iz svih petlji
					failed = true
					break
				} else {
					tempPositionInBlock = 0
					tempPositionInBufferPool++
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
			tempPositionInBufferPool++

		} else {
			continue
		}
	}

	if failed {
		// znači da nam je buffer pool već pun, treba upiasti sve iz poola

		newWalPath := wpo.BlockManager.WriteBufferPoolToWal(wpo.WalManager.Wal.Path) // on već upisuje sve iz buffer poola u wal, napravi nove fajlove i napuni buffer pool
		wpo.WalManager.Wal.Path = newWalPath

		// pozicije positionInBlock i positionInBufferPool su već uradjene

		typeArray = make([][2]uint32, 0)
		compactBytesWritten = uint32(0)
		compactValueCurrentPosition = uint32(0)

		positionInBlock = uint32(0)
		positionInBufferPool = uint32(0)

		complete = false

		for e := wpo.BlockManager.BufferPool.Pool.Front(); e != nil; e = e.Next() {

			if complete {
				break
			}

			if e.Value.(*block_manager.BufferBlock).BlockNumber == positionInBufferPool {

				if positionInBlock+uint32(len(header)) <= blockSize {
					for i := 0; i < len(header); i++ {
						e.Value.(*block_manager.BufferBlock).Data[positionInBlock] = header[i]
						positionInBlock++

						if i == TYPE_START {
							typeArray = append(typeArray, [2]uint32{positionInBufferPool, positionInBlock - 1})
						}
					}
				} else {
					if e.Next() == nil {
						return 3
					} else {
						positionInBlock = 0
						positionInBufferPool++
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
				positionInBufferPool++

			} else {
				continue
			}
		}

		// sada na osnovu typeArray popunjavamo TYPE elemente

		if len(typeArray) == 1 {
			wpo.BlockManager.GetBlockFromBufferPool(typeArray[0][0]).Data[typeArray[0][1]] = 1
		} else {
			wpo.BlockManager.GetBlockFromBufferPool(typeArray[0][0]).Data[typeArray[0][1]] = 2
			wpo.BlockManager.GetBlockFromBufferPool(typeArray[len(typeArray)-1][0]).Data[typeArray[len(typeArray)-1][1]] = 4
			for i := 1; i < len(typeArray)-1; i++ {
				wpo.BlockManager.GetBlockFromBufferPool(typeArray[i][0]).Data[typeArray[i][1]] = 3
			}
		}
	} else {
		for e := bufferPoolCopy.Pool.Front(); e != nil; e = e.Next() {
			wpo.BlockManager.BufferPool.AddBlock(block_manager.NewBufferBlock(e.Value.(*block_manager.BufferBlock).FileName,
				e.Value.(*block_manager.BufferBlock).BlockNumber, e.Value.(*block_manager.BufferBlock).Data))
		}

		bufferPoolCopy = nil

		if len(typeArray) == 1 {
			wpo.BlockManager.GetBlockFromBufferPool(typeArray[0][0]).Data[typeArray[0][1]] = 1
		} else {
			wpo.BlockManager.GetBlockFromBufferPool(typeArray[0][0]).Data[typeArray[0][1]] = 2
			wpo.BlockManager.GetBlockFromBufferPool(typeArray[len(typeArray)-1][0]).Data[typeArray[len(typeArray)-1][1]] = 4
			for i := 1; i < len(typeArray)-1; i++ {
				wpo.BlockManager.GetBlockFromBufferPool(typeArray[i][0]).Data[typeArray[i][1]] = 3
			}
		}
	}

	// sinhronizacija buffer poola sa wal fajlom
	wpo.BlockManager.SyncBufferPoolToWal(wpo.WalManager.Wal.Path)
	return 0
}

func (wpo *WritePath) WriteEntriesToSSTable(entries *[]entry.Entry) uint32 {
	// potrebno je za početak entrije obraditi
	// koristimo varijabilni enkodejer za enkodovanje entrija tj njegovih numeričkih vrednosti

	compactEntries := make([]byte, 0)
	offsets := make([]uint32, 0) // trebaće nam kasnije za indexe

	for _, e := range *entries {
		value, size := encodeEntry(&e)
		compactEntries = append(compactEntries, value...)
		offsets = append(offsets, size)
	}

	fmt.Println(compactEntries)

	sst := sstable.NewEmptySSTable() // sada biramo koji režim upisivanja u sstabelu radimo, merge ili standard

	merge := sst.Merge

	if merge {
		// logika koja se radi kasnije
	} else {
		index := createIndex(entries, offsets, sst.BlockSize)
		fmt.Println("Index ", index)
		fmt.Println("Offsets ", offsets)
		fmt.Println("Entries ", entries)
	}

	return 0
}

// funkcija koja enkodira entry u niz bajtova
func encodeEntry(e *entry.Entry) ([]byte, uint32) { // koristi se za sstable
	encodedEntry := make([]byte, 0)

	encodedEntry = append(encodedEntry, sstable.Uint32toVarint(e.CRC)...)
	encodedEntry = append(encodedEntry, sstable.Uint64toVarint(e.Timestamp)...)
	encodedEntry = append(encodedEntry, e.Tombstone)
	encodedEntry = append(encodedEntry, e.Type)
	encodedEntry = append(encodedEntry, sstable.Uint64toVarint(e.KeySize)...)

	// ako je tombsotne == 1, onda nema potrebe za upisivanjem vrednosti i veličine vrednosti
	if e.Tombstone == byte(0) {
		encodedEntry = append(encodedEntry, sstable.Uint64toVarint(e.ValueSize)...)
		encodedEntry = append(encodedEntry, []byte(e.Key)...)
		encodedEntry = append(encodedEntry, e.Value...)
	} else {
		encodedEntry = append(encodedEntry, []byte(e.Key)...)
	}

	return encodedEntry, uint32(len(encodedEntry))
}

// funkcija koja kreira Index na osnovu bajtova entrija i blocksize-a
func createIndex(entries *[]entry.Entry, offsets []uint32, blockSize uint32) []byte {
	// dakle ideja je napraviti index koji će imati key + zerobyte + offset + newline redove
	// offset je broj bajtova od početka data za taj entry
	// ubacujemo samo ključeve koji se pojavljuju prvi za svaki blok, ne mora biti nužno pozicija 0 u bloku

	fmt.Println("Offsets ", offsets)
	for i := 1; i < len(offsets); i++ { // potrebno je korigovati offsete
		offsets[i] += offsets[i-1]
	}

	fmt.Println("Offsets ", offsets)
	// svaki pomeramo za jedan desno, na prvo mesto ide nula a zadnji element se briše
	for i := len(offsets) - 1; i > 0; i-- {
		offsets[i] = offsets[i-1]
	}
	fmt.Println("Offsets ", offsets)

	offsets[0] = 0
	fmt.Println("Offsets ", offsets)

	index := make([]byte, 0)

	// krećemo iteraciju infiniti blokova, while petlja, krećemo od 0 pa sabiramo (npr 35, 70, 105...)
	// za svaku iteraciju gledamo koji je offset najbliši njemu, a da je pozitivan ili nula
	// kada nadjemo taj offset, gledamo koji je entri key paralelan sa tim offsetom, a sve pre njega ignorišemo
	// prekidamo petlju kada nemamo više offseta koji su veći od trenutne vrednosti bloka

	i := uint32(0)
	for {
		offset := uint32(0)
		found := false
		for j := 0; j < len(offsets); j++ {
			if offsets[j] >= i {
				offset = offsets[j]
				found = true
				break
			}
		}

		if !found {
			break
		}

		for j := 0; j < len(*entries); j++ {
			if offsets[j] == offset {
				index = append(index, []byte((*entries)[j].Key)...)
				index = append(index, 0) // zerobyte
				index = append(index, entry.Uint32ToBytes(uint32(offset) - uint32(offsets[0]))...)
				index = append(index, 10) // newline
				break
			}
		}

		i += blockSize
	}

	return index
}
