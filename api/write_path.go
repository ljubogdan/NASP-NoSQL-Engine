package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/wal"
	"fmt"
	"time"
)

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
	BlockManager *block_manager.BlockManager
	WalManager   *wal.WalManager
}

func NewWritePath() *WritePath {
	return &WritePath{
		BlockManager: block_manager.NewBlockManager(),
		WalManager:   wal.NewWalManager(),
	}
}

func (wpo *WritePath) WriteEntry(key string, value string) {

	minimalRequiredSize := uint32(CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + TYPE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE)
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	entrySize := uint32(minimalRequiredSize + keySize + valueSize)

	blocksPerWal := wpo.WalManager.Wal.BlocksPerWAL
	blockSize := uint32(len(wpo.BlockManager.GetBlockFromBufferPool(0).Data))
	walSize := uint32(blockSize * blocksPerWal)

	if entrySize > walSize {
		fmt.Println("\n" + "\033[31m" + "Entry size exceeds WAL size!" + "\033[0m")
		return
	}

	bufferPoolCopy := block_manager.NewBufferPool()

	for e := wpo.BlockManager.BufferPool.Pool.Front(); e != nil; e = e.Next() {
		fileName := e.Value.(*block_manager.BufferBlock).FileName
		blockNumber := e.Value.(*block_manager.BufferBlock).BlockNumber
		data := e.Value.(*block_manager.BufferBlock).Data
		bufferPoolCopy.AddBlock(block_manager.NewBufferBlock(fileName, blockNumber, data))
	}

	// nadjemo prvu slododnu poziciju u buffer poolu koja je nula bajt 00000000
	// i to uzimamo kao početni položaj za upis

	positionInBlock := uint32(0)
	positionInBufferPool := uint32(0)
	found := false

	for e := wpo.BlockManager.BufferPool.Pool.Front(); e != nil && !found; e = e.Next() {
		for i := 0; i < len(e.Value.(*block_manager.BufferBlock).Data); i++ {
			if e.Value.(*block_manager.BufferBlock).Data[i] == 0 {
				positionInBlock = uint32(i)
				positionInBufferPool = e.Value.(*block_manager.BufferBlock).BlockNumber
				found = true
				break
			}
		}
	}

	crc32 := uint32(entry.CRC32([]byte(key + value)))
	timestamp := uint64(time.Now().UnixNano())
	tombstone := byte(0)
	entryType := byte(0) // za početak nebitno jer je uvek full

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

	for e := bufferPoolCopy.Pool.Front(); e != nil; e = e.Next() {
		if e.Value.(*block_manager.BufferBlock).BlockNumber == tempPositionInBufferPool {

			if tempPositionInBlock+uint32(len(header)) <= blockSize { // pokušamo da upišemo prvo header
				for i := 0; i < len(header); i++ {
					e.Value.(*block_manager.BufferBlock).Data[tempPositionInBlock] = header[i]
					tempPositionInBlock++

					if i == TYPE_START {
						typeArray = append(typeArray, [2]uint32{tempPositionInBufferPool, tempPositionInBlock})
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
				if compactValueCurrentPosition == uint32(len(compactValue)) {
					break
				}
			}

		} else {
			continue
		}
	}

	if failed {
		// druga logika sada ide
	} else {
		// logika ako je sve ok...
	}

}
