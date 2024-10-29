package probabilistics

import (
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
	//defer os.Remove(filename) // Brišemo fajl nakon testa

	deserializedBF, err := DeserializeFromFile(filename)
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
