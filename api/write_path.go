package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/wal"
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
	WalManager          *wal.WalManager
}

func NewWritePath() *WritePath {
	return &WritePath{
		BlockManager: block_manager.NewBlockManager(),
		WalManager:          wal.NewWalManager(),
	}
}

func (wpo *WritePath) WriteEntry(key string, value string) {	// nigde zapravo nemamo objekat entry za sad
	// zatražimo blok od blok menadzera
	// ako je blok poslednji u fajlu i zapis prelazi granice
	// vrati blok blok menadzeru i reci mu da stavi padding, isprazni blokove
	// ako blok nije poslednji, onda 2 slučaja
	// ako u prvi blok sve staje zaključno sa value size
	// upisati bar jedan bajt i probati da upišemo ostatak u sledeći blok (isti postupak)
	// nastavlja se iterativno
	// ako slučajno dodjemo do poslednjeg bloka (2-3 smo prošli) i ne stane sve - problem?????
	// onda smo gubili vreme samo i treba da se vrati blok menadzeru (šta da radi sa njima kasnije??? (ostalo je dosta mesta))
	// koristimo os stat za veličinu blokova jer je O(1)

	// moramo izračunati prvo koliko pri svakom zapisu ode memorije 

}
