package trees

import (
	"testing"
)

func TestMerkleTree(t *testing.T) {
	mt1 := NewMerkleTree()
	mt1.AddBlock(&[]byte{0x01, 0x02, 0x03, 0x02})
	mt1.AddBlock(&[]byte{0x01, 0x02, 0x03, 0x02})
	mt1.AddBlock(&[]byte{0x01, 0x02, 0x03, 0x02})
	mt1.AddBlock(&[]byte{0x01, 0x02, 0x03, 0x02})
	mt1.AddBlock(&[]byte{0x01, 0x02, 0x03, 0x02})
	mt1.Build()

	mt2 := NewMerkleTree()
	mt2.AddBlock(&[]byte{0x01, 0x02, 0x03, 0x12})
	mt2.AddBlock(&[]byte{0x01, 0x02, 0x03, 0x02})
	mt2.AddBlock(&[]byte{0x21, 0x02, 0x03, 0x02})
	mt2.AddBlock(&[]byte{0x01, 0x02, 0x03, 0x02})
	mt2.Build()

	mt3 := Deserialize_MT(mt1.Serialize())

	blocks := [][]byte{
		{0x01, 0x02, 0x03, 0x12},
		{0x01, 0x02, 0x03, 0x02},
		{0x21, 0x02, 0x03, 0x02},
		{0x01, 0x02, 0x03, 0x02},
	}
	mt4 := NewMerkleTreeFromData(&blocks)

	result1 := mt1.Compare(mt2)
	if len(result1) != 3 || result1[0] != 0 || result1[1] != 2 || result1[2] != 4 {
		t.Errorf("Comparison MerkleTree: Expected invalid blocks [0, 2, 4], got %v", result1)
	}

	result2 := mt1.Compare(mt3)
	if len(result2) != 0 {
		t.Errorf("Deserialized MerkleTree: Expected no invalid blocks, got %v", result2)
	}

	result3 := mt2.Compare(mt4)
	if len(result3) != 0 {
		t.Errorf("Build MerkleTree: Building from blocks one by one did not result in the same tree, got %v", result2)
	}
}
