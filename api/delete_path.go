package api


import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/wal"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
	"time"
)

type DeletePath struct {
	BlockManager *block_manager.BlockManager
	WalManager  *wal.WalManager
	MemtableManager *memtable.MemtableManager
}

func NewDeletePath(blockManager *block_manager.BlockManager, walManager *wal.WalManager, memtableManager *memtable.MemtableManager) *DeletePath {
	return &DeletePath{
		BlockManager: blockManager,
		WalManager:  walManager,
		MemtableManager: memtableManager,
	}
}

func (dpo *DeletePath) WriteEntryToWal(key string, value string) uint32 {

	minimalRequiredSize := uint32(CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + TYPE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE)
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	blocksPerWal := dpo.WalManager.Wal.BlocksPerWAL                              // ispraviti kada se bude menjalo u configu
	blockSize := uint32(len(dpo.BlockManager.GetBlockFromBufferPool(0).Data))
	walSize := uint32(blockSize * blocksPerWal)

	if keySize+valueSize > (walSize - (blocksPerWal * minimalRequiredSize)) {
		return 3
	}

	bufferPoolCopy := block_manager.NewBufferPool()

	for e := dpo.BlockManager.BufferPool.Pool.Front(); e != nil; e = e.Next() {
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
	tombstone := byte(1) // sada je obrisan pa je 1
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

		newWalPath := dpo.BlockManager.WriteBufferPoolToWal(dpo.WalManager.Wal.Path) // on već upisuje sve iz buffer poola u wal, napravi nove fajlove i napuni buffer pool
		dpo.WalManager.Wal.Path = newWalPath

		// pozicije positionInBlock i positionInBufferPool su već uradjene

		typeArray = make([][2]uint32, 0)
		compactBytesWritten = uint32(0)
		compactValueCurrentPosition = uint32(0)

		positionInBlock = uint32(0)
		positionInBufferPool = uint32(0)

		complete = false

		for e := dpo.BlockManager.BufferPool.Pool.Front(); e != nil; e = e.Next() {

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
			dpo.BlockManager.GetBlockFromBufferPool(typeArray[0][0]).Data[typeArray[0][1]] = 1
		} else {
			dpo.BlockManager.GetBlockFromBufferPool(typeArray[0][0]).Data[typeArray[0][1]] = 2
			dpo.BlockManager.GetBlockFromBufferPool(typeArray[len(typeArray)-1][0]).Data[typeArray[len(typeArray)-1][1]] = 4
			for i := 1; i < len(typeArray)-1; i++ {
				dpo.BlockManager.GetBlockFromBufferPool(typeArray[i][0]).Data[typeArray[i][1]] = 3
			}
		}
	} else {
		for e := bufferPoolCopy.Pool.Front(); e != nil; e = e.Next() {
			dpo.BlockManager.BufferPool.AddBlock(block_manager.NewBufferBlock(e.Value.(*block_manager.BufferBlock).FileName,
				e.Value.(*block_manager.BufferBlock).BlockNumber, e.Value.(*block_manager.BufferBlock).Data))
		}

		bufferPoolCopy = nil

		if len(typeArray) == 1 {
			dpo.BlockManager.GetBlockFromBufferPool(typeArray[0][0]).Data[typeArray[0][1]] = 1
		} else {
			dpo.BlockManager.GetBlockFromBufferPool(typeArray[0][0]).Data[typeArray[0][1]] = 2
			dpo.BlockManager.GetBlockFromBufferPool(typeArray[len(typeArray)-1][0]).Data[typeArray[len(typeArray)-1][1]] = 4
			for i := 1; i < len(typeArray)-1; i++ {
				dpo.BlockManager.GetBlockFromBufferPool(typeArray[i][0]).Data[typeArray[i][1]] = 3
			}
		}
	}

	// sinhronizacija buffer poola sa wal fajlom
	dpo.BlockManager.SyncBufferPoolToWal(dpo.WalManager.Wal.Path)
	return 0
}

