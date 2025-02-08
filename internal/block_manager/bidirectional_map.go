package block_manager

import (
	"bytes"
	"encoding/binary"
)

// mapa koja se koristi za globalni rečnik
/*
U polju key ne čuva se konkretna vrednost ključa,
već numerička vrednost koja mu je dodeljena u globalnom rečniku kompresije
koji postoji na nivou svih SSTabela
*/

type BidirectionalMap struct {
	ForwardMap map[string]uint32
	ReverseMap map[uint32]string
	Counter    uint32
}

func NewBidirectionalMap() *BidirectionalMap {
	return &BidirectionalMap{
		ForwardMap: make(map[string]uint32),
		ReverseMap: make(map[uint32]string),
		Counter:    1,
	}
}

// funkcija koja dodaje ključ u mapu
func (bdm *BidirectionalMap) Add(key string) uint32 {
	// ključ se dodaje u mapu samo ako već ne postoji
	if _, ok := bdm.ForwardMap[key]; !ok {
		bdm.ForwardMap[key] = bdm.Counter
		bdm.ReverseMap[bdm.Counter] = key
		bdm.Counter++
	}

	return bdm.ForwardMap[key]
}

// funkcija koja dobavlja uint32 vrednost ključa na osnovu stringa
func (bdm *BidirectionalMap) GetByString(key string) uint32 {
	if val, ok := bdm.ForwardMap[key]; ok {
		return val
	}
	return 0
}

// funkcija koja dobavlja string vrednost ključa na osnovu uint32
func (bdm *BidirectionalMap) GetByUint32(key uint32) string {
	if val, ok := bdm.ReverseMap[key]; ok {
		return val
	}
	return ""
}

// funkcija koja serijalizuje mapu u niz bajtova
// kasnije koristimo za upis u bin fajl i imaćemo funkciju za deserijalizaciju iz fajla
func (bdm *BidirectionalMap) SerializeBidirectionalMap() []byte {
	var buffer bytes.Buffer

	// prvo serijalizujemo brojač
	binary.Write(&buffer, binary.BigEndian, bdm.Counter)

	// zatim serijalizujemo ključeve i vrednosti
	for key, value := range bdm.ForwardMap {
		keySize := uint32(len(key))
		binary.Write(&buffer, binary.BigEndian, keySize)
		buffer.Write([]byte(key))
		binary.Write(&buffer, binary.BigEndian, value)
	}

	return buffer.Bytes()
}

// funkcija koja deserijalizuje mapu iz niza bajtova
func DeserializeBidirectionalMap(data []byte) *BidirectionalMap {
	bdm := NewBidirectionalMap()

	reader := bytes.NewReader(data)

	// prvo deserijalizujemo brojač
	err := binary.Read(reader, binary.BigEndian, &bdm.Counter)
	HandleError(err, "Failed to deserialize counter")

	for reader.Len() > 0 {
		var keySize uint32
		err = binary.Read(reader, binary.BigEndian, &keySize)
		HandleError(err, "Failed to deserialize key size")

		keyBytes := make([]byte, keySize)
		_, err = reader.Read(keyBytes)
		HandleError(err, "Failed to read key")

		key := string(keyBytes)

		var value uint32
		err = binary.Read(reader, binary.BigEndian, &value)
		HandleError(err, "Failed to deserialize value")

		bdm.ForwardMap[key] = value
		bdm.ReverseMap[value] = key
	}

	return bdm
}




	



	
	
