package block_manager

// ideja je implementirati cache pool da radi sa entrijima
// ali da immamo implementaciju liste i mape istovremeno
// odnosno da mapa mapira ključ koji je recimo sstable_xxxxx-blockNumber na element u listi
// a lista da čuva sve elemente

// lista koristi LRU mehanizam, tako da se elementi koji se najmanje koriste izbacuju

import (
	"NASP-NoSQL-Engine/internal/config"
	"container/list"
)

type CachePool struct {
	Capacity uint32
	Pool     *list.List
}

func NewCachePool() *CachePool {
	return &CachePool{
		Capacity: config.ReadCachePoolCapacity(),
		Pool:     list.New(),
	}
}