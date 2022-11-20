package consistenthash

import (
	"github.com/hodis/lib/logger"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

/*
https://www.cnblogs.com/Finley/p/14038398.html
一致性 hash 算法的目的是在节点数量 n 变化时,
使尽可能少的 key 需要进行节点间重新分布。
一致性 hash 算法将数据 key 和服务器地址 addr 散列到 2^32 的空间中。
 */

type HashFunc func(data []byte) uint32

type Map struct {
	hashFunc HashFunc
	replicas int  // 每个物理节点会产生 replicas 个虚拟节点
	keys []int
	hashMap map[int]string
}


func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas: replicas,
		hashFunc: fn,
		hashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns if there is no node in Map
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

/*
传入的是集群节点的名字
 */
func (m *Map) AddNode(keys ...string) {
	logger.Info("AddNode keys: ", keys)
	for _, key := range keys {
		if key == "" {
			continue
		}
		// 遍历一个物理节点上的所有虚拟节点
		for i:=0;i<m.replicas;i++ {
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

func getPartitionKey(key string) string {
	begin := strings.Index(key, "{")
	if begin == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == begin + 1 {
		return key
	}
	return key[begin+1:end]
}

func (m *Map) PickNode(key string) string {
	if m.IsEmpty() {
		logger.Info("m is empty")
		return ""
	}
	partitionkey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(partitionkey)))

	/*
		返回在 m.keys 数组中第一个 >= hash 的元素的索引
		如果不存在这样一个元素，那么返回 len(m.keys)
	*/
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	logger.Info("PickNode, idx: ", idx)
	logger.Info("PickNode, len(m.keys): ", len(m.keys))
	// 如果当前没有任何元素 >= hash, 那么就默认返回第一个节点
	if idx == len(m.keys) {
		idx = 0
	}
	return m.hashMap[m.keys[idx]]
}
