package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

type shard struct {
	m map[string]interface{}
	mutex sync.RWMutex
}

/*
ConcurrentDict 是实现了 Dict 接口的
 */
type ConcurrentDict struct {
	table []*shard
	count int32
	shardCount int
}

/*
这个函数用来返回一个 大于 param 且是最小 2 的幂次方
例如 2^4 < n < 2^5, 那么传入 n，就会返回 2^5
*/
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	size := computeCapacity(shardCount)
	table := make([]*shard, size)

	for i:=0;i<size;i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}

	return &ConcurrentDict{
		table:      table,
		count:      0,
		shardCount: size,
	}
}

const prime32 = uint32(16777619)

// 用来 key 进行 hash 的函数
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

/*
为传入的 key 的 hashcode 分配一个槽，返回的是槽的索引
当容量 n 是 2 的幂次方，那么 h % n == (n-1) & h
 */
func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	n := uint32(len(dict.table))
	return hashCode & (n-1)
}

func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	// 先获取 key 的 hash code
	hashCode := fnv32(key)
	// 通过 hashcode 获取槽（shard）
	s := dict.getShard(dict.spread(hashCode))

	s.mutex.RLock()
	defer s.mutex.RUnlock()
	val, exists = s.m[key]
	return
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	// 先利用 key 计算出槽的位置
	hashCode := fnv32(key)
	s := dict.getShard(dict.spread(hashCode))

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}

	s.m[key] = val
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	// 先利用 key 计算出槽的位置
	hashCode := fnv32(key)
	s := dict.getShard(dict.spread(hashCode))

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}

	s.m[key] = val
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	// 先利用 key 计算出槽的位置
	hashCode := fnv32(key)
	s := dict.getShard(dict.spread(hashCode))

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// key 存在，才更新
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) Remove(key string) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	hashCode := fnv32(key)
	s := dict.getShard(dict.spread(hashCode))

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, s := range dict.table {
		s.mutex.Lock()
		func() {
			defer s.mutex.Unlock()
			for k, v := range s.m {
				isContinue := consumer(k, v)
				if !isContinue {
					return
				}
			}
		}()
	}
}

func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

func (s *shard) RandomKey() string {
	if s == nil {
		panic("shard is nil")
	}
	for key := range s.m {
		return key
	}
	return ""
}

func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	if dict == nil {
		panic("dict is nil")
	}

	if limit >= dict.Len() {
		return dict.Keys()
	}

	shardCount := len(dict.table)

	keys := make([]string, limit)


	for i:=0;i<limit;i++ {
		// 随机选一个槽 shard
		s := dict.getShard(uint32(rand.Intn(shardCount)))
		if s == nil {
			continue
		}
		result := s.RandomKey()
		if result != "" {
			keys[i] = result
			i++
		}
	}
	return keys
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	if limit >= dict.Len() {
		return dict.Keys()
	}

	result := make([]string, limit)
	temp := make(map[string]bool)
	shardCount := len(dict.table)

	for len(temp) < limit {
		s := dict.getShard(uint32(rand.Intn(shardCount)))
		k := s.RandomKey()
		if k != "" {
			temp[k] = true
		}
	}

	i := 0
	for k, _ := range temp {
		result[i] = k
		i++
	}
	return result
}

func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}

/*
能够保证多线程下，dict.count 累加的正确性
 */
func (dict *ConcurrentDict) addCount() {
	atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}
