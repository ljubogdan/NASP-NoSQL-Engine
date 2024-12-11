package trees

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"testing"
)

func TestMerkleTree(t *testing.T) {
	blocks1 := []block_manager.BufferBlock{
		{FileName: "", BlockNumber: 0, Data: []byte{0x01, 0x02, 0x03, 0x02}},
		{FileName: "", BlockNumber: 0, Data: []byte{0x01, 0x02, 0x03, 0x02}},
		{FileName: "", BlockNumber: 0, Data: []byte{0x01, 0x02, 0x03, 0x02}},
		{FileName: "", BlockNumber: 0, Data: []byte{0x01, 0x02, 0x03, 0x02}},
		{FileName: "", BlockNumber: 0, Data: []byte{0x01, 0x02, 0x03, 0x02}}}
	blocks2 := []block_manager.BufferBlock{
		{FileName: "", BlockNumber: 0, Data: []byte{0x01, 0x02, 0x03, 0x12}},
		{FileName: "", BlockNumber: 0, Data: []byte{0x01, 0x02, 0x03, 0x02}},
		{FileName: "", BlockNumber: 0, Data: []byte{0x21, 0x02, 0x03, 0x02}},
		{FileName: "", BlockNumber: 0, Data: []byte{0x01, 0x02, 0x03, 0x02}},
		{FileName: "", BlockNumber: 0, Data: []byte{0x07, 0x02, 0x03, 0x02}}}

	mt1 := NewMerkleTree(&blocks1)
	mt2 := NewMerkleTree(&blocks2)
	mt3 := Deserialize_MT(mt1.Serialize())

	result1 := mt1.Compare(mt2)
	if len(result1) != 3 || result1[0] != 0 || result1[1] != 2 || result1[2] != 4 {
		t.Errorf("Comparison MerkleTree: Expected invalid blocks [0, 2, 4], got %v", result1)
	}

	result2 := mt1.Compare(mt3)
	if len(result2) != 0 {
		t.Errorf("Deserialized MerkleTree: Expected no invalid blocks, got %v", result2)
	}
}
