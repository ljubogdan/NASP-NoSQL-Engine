package api

import (
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/sstable"
	"fmt"
)

type RangeScan struct {
	ReadPathObject  *ReadPath
	iterators       []sstable.SSTableIterator
	memtableEntries []entry.Entry
	sstableEntries  []entry.Entry
	pageSize        uint32
	min             string
	max             string
}

func NewRangeScan(rpo *ReadPath, min string, max string) *RangeScan {
	sstIterators := *rpo.GetStartingIteratorsForRange(min, max)
	sstEntries := make([]entry.Entry, len(sstIterators))
	for i := 0; i < len(sstEntries); i++ {
		if sstEntries[i].Key != sstIterators[i].LastKey {
			e, exists := rpo.FindInDataByIterator(&sstIterators[i])
			sstEntries[i] = e
			if exists && sstEntries[i].Key <= max {
				continue
			}
		}

		sstEntries = append(sstEntries[0:i], sstEntries[i+1:]...)
		sstIterators = append(sstIterators[0:i], sstIterators[i+1:]...)
	}

	memtbEntries := *rpo.MemtableManager.GetAll()
	memtbStart := -1
	memtbEnd := len(memtbEntries)
	for i := 0; i < len(memtbEntries); i++ {
		if memtbStart == -1 && memtbEntries[i].Key >= min {
			memtbStart = i
			memtbEnd = len(memtbEntries)
		}
		if memtbEntries[i].Key > max {
			memtbEnd = i
			break
		}
	}
	if memtbStart != -1 {
		memtbEntries = memtbEntries[memtbStart:memtbEnd]
	} else {
		memtbEntries = make([]entry.Entry, 0)
	}
	fmt.Println(memtbEntries)

	return &RangeScan{
		ReadPathObject:  rpo,
		iterators:       sstIterators,
		memtableEntries: memtbEntries,
		sstableEntries:  sstEntries,
		pageSize:        config.ReadScanPageSize(),
		min:             min,
		max:             max,
	}
}

func (rs *RangeScan) NextPage() *[]entry.Entry {
	pageEntries := make([]entry.Entry, 0)

	for (len(rs.memtableEntries) > 0 || len(rs.sstableEntries) > 0) && len(pageEntries) < int(rs.pageSize) {
		for i := 0; i < len(rs.sstableEntries); i++ {
			fmt.Println(rs.iterators[i].SSTableName + " " + rs.sstableEntries[i].Key)
		}
		fmt.Println()
		min := ""
		minIndex := 0
		if len(rs.memtableEntries) > 0 {
			min = rs.memtableEntries[0].Key
			minIndex = -1
		}

		for i := 0; i < len(rs.sstableEntries); i++ {
			if min == "" || rs.sstableEntries[i].Key < min {
				min = rs.sstableEntries[i].Key
				minIndex = i
			} else if min == rs.sstableEntries[i].Key {
				if rs.FetchNextEntry(i) {
					i--
				}
			}
		}

		if minIndex == -1 {
			if rs.memtableEntries[0].Tombstone != 1 {
				pageEntries = append(pageEntries, rs.memtableEntries[0])
			}
			rs.memtableEntries = rs.memtableEntries[1:]
		} else {
			if rs.sstableEntries[minIndex].Tombstone != 1 {
				pageEntries = append(pageEntries, rs.sstableEntries[minIndex])
			}
			rs.FetchNextEntry(minIndex)
		}
	}

	return &pageEntries
}

func (rs *RangeScan) FetchNextEntry(iteratorIndex int) bool {
	if rs.sstableEntries[iteratorIndex].Key != rs.iterators[iteratorIndex].LastKey {
		e, exists := rs.ReadPathObject.FindInDataByIterator(&rs.iterators[iteratorIndex])
		rs.sstableEntries[iteratorIndex] = e
		if exists && rs.sstableEntries[iteratorIndex].Key <= rs.max {
			return false
		}
	}

	rs.sstableEntries = append(rs.sstableEntries[0:iteratorIndex], rs.sstableEntries[iteratorIndex+1:]...)
	rs.iterators = append(rs.iterators[0:iteratorIndex], rs.iterators[iteratorIndex+1:]...)
	return true
}
