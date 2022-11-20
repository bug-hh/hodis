package sortedset

import (
	"github.com/hodis/lib/logger"
	"strconv"
)

// SortedSet is a set which keys sorted by bound score
/*
有序集合同时使用字典和 skiplist 构成自己
这么做的原因是为了同时利用这两种数据结果的优点
dict 能够在 O(1) 时间内拿到对应成员的 score
在 zrange 这种场景下，需要把一批成员有序排列并返回，这个要靠 skiplist
 */
type SortedSet struct {
	// 字典以成员作为 key，score 作为 value（这里是 element）
	dict     map[string]*Element
	skiplist *skiplist
}

func Make() *SortedSet {
	return &SortedSet{
		dict: make(map[string]*Element),
		skiplist: makeSkiplist(),
	}
}

func (sortedSet *SortedSet) Remove(member string) bool {
	v, ok := sortedSet.dict[member]
	if ok {
		sortedSet.skiplist.remove(member, v.Score)
		delete(sortedSet.dict, member)
		return true
	}
	return false
}

func (sortedSet *SortedSet) Get(member string) (element *Element, ok bool) {
	element, ok = sortedSet.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

func (sortedSet *SortedSet) GetSkipList() *skiplist {
	return sortedSet.skiplist
}

// Add puts member into set,  and returns whether has inserted new node
func (sortedSet *SortedSet) Add(member string, score float64) bool {
	element, ok := sortedSet.dict[member]
	sortedSet.dict[member] = &Element{
		Member: member,
		Score: score,
	}
	// 这个成员本来就存在，那么就更新分数 score
	if ok {
		if score != element.Score {
			// 先从 skiplist 中删除元素
			sortedSet.skiplist.remove(member, element.Score)
			// 再重新插入 skiplist
			sortedSet.skiplist.insert(member, score)
		}
		// 并没有新增，只是更新，所以返回 false
		return false
	}
	sortedSet.skiplist.insert(member, score)
	return true
}

// Len returns number of members in set
func (sortedSet *SortedSet) Len() int64 {
	logger.Debug("Len - dict: ", sortedSet.dict)
	return int64(len(sortedSet.dict))
}

func (sortedSet *SortedSet) ForEach(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := int64(sortedSet.Len())
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		panic("illegal end " + strconv.FormatInt(stop, 10))
	}

	// find start node
	var node *node
	// desc 代表是否降序排列
	if desc {
		node = sortedSet.skiplist.tail
		if start > 0 {
			node = sortedSet.skiplist.getByRank(int64(size - start))
		}
	} else {
		node = sortedSet.skiplist.header.level[0].forward
		if start > 0 {
			node = sortedSet.skiplist.getByRank(int64(start + 1))
		}
	}

	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

func (sortedSet *SortedSet) Range(start int64, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	sortedSet.ForEach(start, stop, desc, func(element *Element) bool {
		if element == nil {
			logger.Debug("element is nil")
		}
		slice[i] = element
		i++
		return true
	})
	return slice
}


