package block_manager

// ideja je implementirati cache pool da radi sa entrijima
// ali da immamo implementaciju liste i mape istovremeno
// odnosno da mapa mapira ključ koji je recimo sstable_xxxxx-blockNumber na element u listi
// a lista da čuva sve elemente

// lista koristi LRU mehanizam, tako da se elementi koji se najmanje koriste izbacuju

/*
Cache implementirati koristeći LRU algoritam
Korisnik može da specificira maksimalnu veličinu keša
Potrebno je voditi računa o tome da keš ne sadrži zastarele verzije podataka i da se ispravno ažurira prilikom svake operacije čitanja
*/

import (
	"NASP-NoSQL-Engine/internal/config"
	"container/list"
)

type CachePool struct {
	Capacity uint32
	Cache   map[string]*list.Element // mapa koja mapira ključ na element u listi
	List    *list.List
}

func NewCachePool() *CachePool {
	return &CachePool{
		Capacity: config.ReadCachePoolCapacity(),
		Cache:   make(map[string]*list.Element),
		List:    list.New(),
	}
}

// NAPOMENA: CACHE POOL čuva CacheEntry objekte

// dodavanje elementa u keš
func (pool *CachePool) Add(cacheEntry *CacheEntry) {

	// pre nego što ubacimo element proverimo da li postoje zastarele verzije podatka sa istim ključem
	// i obrišemo ih
	if element, ok := pool.Cache[cacheEntry.Key]; ok {
		pool.List.Remove(element)
		delete(pool.Cache, cacheEntry.Key)
	}

	// ako je keš pun, uklonimo LeastUsed element
	if uint32(pool.List.Len()) >= pool.Capacity {
		pool.RemoveLeastUsed()
	}

	element := pool.List.PushFront(cacheEntry)
	pool.Cache[cacheEntry.Key] = element
}

// uklanjanje LeastUsed elementa
func (pool *CachePool) RemoveLeastUsed() {
	element := pool.List.Back()
	if element != nil {
		cacheEntry := element.Value.(*CacheEntry)
		delete(pool.Cache, cacheEntry.Key)
		pool.List.Remove(element)
	}
}

// pronalaženje elementa u kešu
func (pool *CachePool) Get(key string) *CacheEntry {
	if element, ok := pool.Cache[key]; ok {
		pool.List.MoveToFront(element)
		return element.Value.(*CacheEntry)
	}
	return nil
}

// brisanje elementa iz keša
func (pool *CachePool) Delete(key string) {
	if element, ok := pool.Cache[key]; ok {
		cacheEntry := element.Value.(*CacheEntry)
		delete(pool.Cache, cacheEntry.Key)
		pool.List.Remove(element)
	}
}

// funkcionalnost koja se koristi za put i delete operaciju
// a to je da se updatuje vrednost u kešu samo ako je element već u kešu
func (pool *CachePool) UpdateIfPresent(key string, value []byte) {
	// getujemo element iz keša koristeći funkciju Get
	// vremenska složenost je O(1) jer koristimo mapu
	present := pool.Get(key)
	if present != nil {
		present.Value = value
	}
}
