package probabilistics

import (
	"os"
	"testing"
)

// go test -v ./internal/probabilistics -run TestBloomFilter

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.Add("hello")
	bf.Add("world")
	bf.Add("!")

	if !bf.Contains("hello") {
		t.Errorf("Bloom filter doesn't contain 'hello'")
	}

	if !bf.Contains("world") {
		t.Errorf("Bloom filter doesn't contain 'world'")
	}

	if !bf.Contains("!") {
		t.Errorf("Bloom filter doesn't contain '!'")
	}

	if bf.Contains("foo") {
		t.Errorf("Bloom filter contains 'foo'")
	}

	if bf.Contains("bar") {
		t.Errorf("Bloom filter contains 'bar'")
	}

	filename := "bloomfilter_test.dat"
	if err := bf.SerializeToFile(filename); err != nil {
		t.Fatalf("failed to serialize Bloom filter: %v", err)
	}
	defer os.Remove(filename)

	deserializedBF, err := DeserializeFromFile_BF(filename)
	if err != nil {
		t.Fatalf("failed to deserialize Bloom filter: %v", err)
	}

	if !deserializedBF.Contains("hello") {
		t.Errorf("Deserialized Bloom filter doesn't contain 'hello'")
	}

	if !deserializedBF.Contains("world") {
		t.Errorf("Deserialized Bloom filter doesn't contain 'world'")
	}

	if !deserializedBF.Contains("!") {
		t.Errorf("Deserialized Bloom filter doesn't contain '!'")
	}

	if deserializedBF.Contains("foo") {
		t.Errorf("Deserialized Bloom filter contains 'foo'")
	}

	if deserializedBF.Contains("bar") {
		t.Errorf("Deserialized Bloom filter contains 'bar'")
	}
}













// go test -v ./internal/probabilistics -run TestCountMinSketch

func TestCountMinSketch(t *testing.T) {
	cms := NewCountMinSketch(0.01, 0.01)

	cms.Add("apple")
	cms.Add("banana")
	cms.Add("apple")
	cms.Add("orange")
	cms.Add("banana")
	cms.Add("apple")

	if count := cms.Count("apple"); count < 3 {
		t.Errorf("Expected 'apple' count to be at least 3, got %d", count)
	}

	if count := cms.Count("banana"); count < 2 {
		t.Errorf("Expected 'banana' count to be at least 2, got %d", count)
	}

	if count := cms.Count("orange"); count < 1 {
		t.Errorf("Expected 'orange' count to be at least 1, got %d", count)
	}

	if count := cms.Count("grape"); count != 0 {
		t.Errorf("Expected 'grape' count to be 0 or close to 0, got %d", count)
	}

	filename := "countminsketch_test.dat"
	if err := cms.SerializeToFile(filename); err != nil {
		t.Fatalf("Failed to serialize Count-Min Sketch: %v", err)
	}
	defer os.Remove(filename)

	deserializedCMS, err := DeserializeFromFile_CMS(filename)
	if err != nil {
		t.Fatalf("Failed to deserialize Count-Min Sketch: %v", err)
	}

	if count := deserializedCMS.Count("apple"); count < 3 {
		t.Errorf("Deserialized Count-Min Sketch: Expected 'apple' count to be at least 3, got %d", count)
	}

	if count := deserializedCMS.Count("banana"); count < 2 {
		t.Errorf("Deserialized Count-Min Sketch: Expected 'banana' count to be at least 2, got %d", count)
	}

	if count := deserializedCMS.Count("orange"); count < 1 {
		t.Errorf("Deserialized Count-Min Sketch: Expected 'orange' count to be at least 1, got %d", count)
	}

	if count := deserializedCMS.Count("grape"); count != 0 {
		t.Errorf("Deserialized Count-Min Sketch: Expected 'grape' count to be 0 or close to 0, got %d", count)
	}
}
