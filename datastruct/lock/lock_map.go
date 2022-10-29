package lock

import (
	"sort"
	"sync"
)

const (
	prime32 = uint32(16777619)
)

type Locks struct {
	table []*sync.RWMutex
}

func Make(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i:=0;i<tableSize;i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{table: table}
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (lock *Locks) spread(hashCode uint32) uint32 {
	if lock == nil {
		panic("lock is nil")
	}
	tableSize := uint32(len(lock.table))
	return hashCode & (tableSize - 1)
}

/*
把 key 转换成 index，并排序
 */
func (lock *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexSet := make(map[uint32]bool)
	for _, key := range keys {
		hashCode := fnv32(key)
		index := lock.spread(hashCode)
		indexSet[index] = true
	}

	indexSlice := make([]uint32, 0, len(indexSet))

	for index, _ := range indexSet {
		indexSlice = append(indexSlice, index)
	}

	sort.Slice(indexSlice, func(i, j int) bool {
		if reverse {
			return indexSlice[i] > indexSlice[j]
		}
		return indexSlice[i] < indexSlice[j]
	})
	return indexSlice
}

// 允许 writeKeys 和 readKeys 里有重复的元素
func (lock *Locks) RWLocks(writeKeys []string, readKeys []string) {
	var keys []string
	keys = append(keys, writeKeys...)
	keys = append(keys, readKeys...)

	keyIndices := lock.toLockIndices(keys, false)
	writeKeyIndices := lock.toLockIndices(writeKeys, false)

	writeKeysMap := make(map[uint32]struct{})

	for _, index := range writeKeyIndices {
		/*
			struct{}表示一个空的结构体，
			注意，直接定义一个空的结构体并没有意义，
			但在并发编程中，channel之间的通讯，可以使用一个struct{}作为信号量。
		*/
		writeKeysMap[index] = struct{}{}
	}

	for _, index := range keyIndices {
		_, ok := writeKeysMap[index]
		if ok {
			lock.table[index].Lock()
		} else {
			lock.table[index].RLock()
		}
	}
}

// 允许 writeKeys 和 readKeys 里有重复的元素
func (lock *Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	var keys []string
	keys = append(keys, writeKeys...)
	keys = append(keys, readKeys...)

	keyIndices := lock.toLockIndices(keys, false)
	writeKeyIndices := lock.toLockIndices(writeKeys, false)

	writeKeysMap := make(map[uint32]struct{})

	for _, index := range writeKeyIndices {
		/*
			struct{}表示一个空的结构体，
			注意，直接定义一个空的结构体并没有意义，
			但在并发编程中，channel之间的通讯，可以使用一个struct{}作为信号量。
		*/
		writeKeysMap[index] = struct{}{}
	}

	for _, index := range keyIndices {
		_, ok := writeKeysMap[index]
		if ok {
			lock.table[index].Unlock()
		} else {
			lock.table[index].RUnlock()
		}
	}
}