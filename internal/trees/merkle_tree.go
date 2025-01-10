package trees

import (
	"crypto/sha256"
	"sync"
)

type MerkleTree struct {
	hashes       []byte
	levelIndexes []uint16
}

func NewMerkleTree() *MerkleTree {
	return &MerkleTree{hashes: make([]byte, 0), levelIndexes: make([]uint16, 0)}
}

func NewMerkleTreeFromData(data *[][]byte) *MerkleTree {
	blockCount := len(*data)
	hashCount := blockCount
	height := 0
	for bpl := blockCount; bpl > 0; bpl /= 2 {
		height++
		bpl++
		if bpl == 2 {
			break
		}
		hashCount += bpl / 2
	}

	merkle := &MerkleTree{hashes: make([]byte, hashCount*32), levelIndexes: make([]uint16, height)}

	var wg sync.WaitGroup
	for i := 0; i < blockCount; i++ {
		wg.Add(1)
		merkle.writeHashAsync(&(*data)[i], hashCount-blockCount+i, 0x01, &wg)
	}
	wg.Wait()

	merkle.generateTree(hashCount, blockCount, height)
	return merkle
}

func (merkle *MerkleTree) AddBlock(block *[]byte) {
	index := len(merkle.hashes) / 32
	merkle.hashes = append(merkle.hashes, make([]byte, 32)...)
	merkle.writeHash(block, index, 0x01)
}

func (merkle *MerkleTree) writeHash(data *[]byte, index int, separator byte) {
	dataWithSeparator := append([]byte{separator}, (*data)[:]...) // Dodaj separator na kraj pre hash-ovanja zbog razdvajanja nivoa
	blockHash := sha256.Sum256(dataWithSeparator)
	for i := 0; i < 32; i++ {
		merkle.hashes[index*32+i] = blockHash[i] // Verovatno postoji efikasniji način da se prepišu svi byte-ovi hash-a
	}
}

func (merkle *MerkleTree) writeHashAsync(data *[]byte, index int, separator byte, wg *sync.WaitGroup) {
	defer wg.Done()
	dataWithSeparator := append([]byte{separator}, (*data)[:]...) // Dodaj separator na kraj pre hash-ovanja zbog razdvajanja nivoa
	blockHash := sha256.Sum256(dataWithSeparator)
	for i := 0; i < 32; i++ {
		merkle.hashes[index*32+i] = blockHash[i] // Verovatno postoji efikasniji način da se prepišu svi byte-ovi hash-a
	}
}

func (merkle *MerkleTree) generateTree(levelEnd int, blockCount int, height int) {
	levelCount := 1
	for levelEnd > 1 {
		lastLevel := levelEnd
		levelEnd -= blockCount
		if blockCount > 1 {
			blockCount = (blockCount + 1) / 2
		}
		merkle.levelIndexes[height-levelCount] = uint16(levelEnd)
		levelCount++
		var wg sync.WaitGroup
		for i := 0; i < blockCount; i++ {
			var combinedHash []byte
			var separator byte              // Dodaje se separator da razlikujemo stabla sa istim root-om, a raličitom dubinom ili brojem blokova
			if levelEnd+i*2+2 > lastLevel { // Ukoliko predhodan nivo nema 2 cvora, dopuni
				combinedHash = append(make([]byte, 32), merkle.hashes[(levelEnd+i*2)*32:(levelEnd+i*2+1)*32]...)
				separator = byte(levelCount - 1) // Smanjiti nivo separatora ako se koristi prazan blok za dopunu
			} else {
				combinedHash = merkle.hashes[(levelEnd+i*2)*32 : (levelEnd+i*2+2)*32]
				separator = byte(levelCount)
			}
			wg.Add(1)
			go merkle.writeHashAsync(&combinedHash, levelEnd-blockCount+i, separator, &wg) // Druga nit hash-uje i upisuje hash u predodređen index
		}
		wg.Wait()
	}
}

func (merkle *MerkleTree) Build() {
	blockCount := len(merkle.hashes) / 32
	hashCount := blockCount
	height := 0
	for bpl := blockCount; bpl > 0; bpl /= 2 {
		height++
		bpl++
		if bpl == 2 {
			break
		}
		hashCount += bpl / 2
	}

	merkle.hashes = append(make([]byte, (hashCount-blockCount)*32), merkle.hashes...)
	merkle.levelIndexes = make([]uint16, height)

	merkle.generateTree(hashCount, blockCount, height)
}

func (merkle *MerkleTree) compareHash(other *MerkleTree, index uint16) bool {
	for i := uint16(0); i < 32; i++ {
		if merkle.hashes[index+i] != other.hashes[index+i] {
			return false
		}
	}
	return true
}

func (merkle *MerkleTree) compareSubtree(other *MerkleTree, level int, levelIndex uint16) []uint16 {
	if merkle.compareHash(other, (merkle.levelIndexes[level]+levelIndex)*32) { // Ako se hash poklapa ostatak stabla je sigurno dobar
		return []uint16{}
	} else if level >= len(merkle.levelIndexes)-1 { // Ako se hash lista ne poklapa, vrati poziciju lista
		return []uint16{levelIndex}
	}

	blocks := (merkle.compareSubtree(other, level+1, 2*levelIndex)) // Zabeleži neispravne blokove levog podstabla
	rightIndex := merkle.levelIndexes[level+1] + 2*levelIndex + 1
	if rightIndex < uint16(len(merkle.hashes)/32) && (level == len(merkle.levelIndexes)-2 || rightIndex < merkle.levelIndexes[level+2]) {
		return append(blocks, merkle.compareSubtree(other, level+1, 2*levelIndex+1)...) // Ako postoje desni vrati uniju levih i desnih neispravnih blokova
	}
	return blocks
}

func (merkle *MerkleTree) Compare(other *MerkleTree) []uint16 {
	return merkle.compareSubtree(other, 0, 0)
}

func (merkle *MerkleTree) Serialize() *[]byte {
	blocks := merkle.hashes[merkle.levelIndexes[len(merkle.levelIndexes)-1]*32:]
	return &blocks
}

func Deserialize_MT(data *[]byte) *MerkleTree {
	blockCount := len(*data) / 32
	hashCount := blockCount
	height := 0
	for bpl := blockCount; bpl > 0; bpl /= 2 {
		height++
		bpl++
		if bpl == 2 {
			break
		}
		hashCount += bpl / 2
	}

	merkle := &MerkleTree{hashes: make([]byte, hashCount*32), levelIndexes: make([]uint16, height)}
	for i := 0; i < blockCount*32; i++ {
		merkle.hashes[(hashCount-blockCount)*32+i] = (*data)[i]
	}

	merkle.generateTree(hashCount, blockCount, height)
	return merkle
}
