package sortedset

import (
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"math"
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


func cannotUpdate(scoreUpdatePolicy int, newScore float64, oldScore float64) bool {
	return (scoreUpdatePolicy == utils.SORTED_SET_UPDATE_GREATER && newScore <= oldScore) ||
		scoreUpdatePolicy == utils.SORTED_SET_UPDATE_LESS_THAN && newScore >= oldScore
}

// Add puts member into set,  and returns whether has inserted new node
func (sortedSet *SortedSet) Add(member string, score float64, policy int, scoreUpdatePolicy int, isCH, isINCR bool) bool {
	element, ok := sortedSet.dict[member]
	// 这个成员本来就存在，那么就更新分数 score
	if ok {
		if policy == utils.InsertPolicy || cannotUpdate(scoreUpdatePolicy, score, element.Score) {
			return false
		}

		if isINCR {
			score += element.Score
		}

		if score == element.Score {
			return false
		}

		logger.Debug("INSERT OK")
		// 先从 skiplist 中删除元素
		sortedSet.skiplist.remove(member, element.Score)
		// 再重新插入 skiplist
		sortedSet.skiplist.insert(member, score)

		sortedSet.dict[member] = &Element{
			Member: member,
			Score: score,
		}
		// 在没有指定 isCH 选项的情况下，并没有新增，只是更新，所以返回 false
		if !isCH {
			return false
		}
		return true
	}
	if policy == utils.UpdatePolicy {
		logger.Debug("not ok return false")
		return false
	}
	sortedSet.skiplist.insert(member, score)
	sortedSet.dict[member] = &Element{
		Member: member,
		Score: score,
	}
	return true
}

// Len returns number of members in set
func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict))
}

func (sortedSet *SortedSet) ForEach(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := sortedSet.Len()
	if size == 0 {
		return
	}
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
		node = sortedSet.skiplist.Tail
		if start > 0 {
			node = sortedSet.skiplist.getByRank(size - start)
		}
	} else {
		node = sortedSet.skiplist.Header.level[0].forward
		if start > 0 {
			node = sortedSet.skiplist.getByRank(start + 1)
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

func (sortedSet *SortedSet) Rank(member string, desc bool) int {
	var start, stop int64
	start = 0
	stop = sortedSet.Len()
	i := 0
	ret := -1
	sortedSet.ForEach(start, stop, desc, func(element *Element) bool {
		if member == element.Member {
			ret = i
		}
		i++
		return true
	})
	return ret
}

func Unions(sets []*SortedSet, weights []float64, aggregate int) *SortedSet {
	ret := Make()
	for i, set := range sets {
		if set == nil {
			continue
		}
		set.ForEach(0, set.Len(), false, func(element *Element) bool {
			item, exists := ret.Get(element.Member)
			score := element.Score * weights[i]
			if exists {
				if aggregate == utils.AGGREGATE_SUM {
					score += item.Score
				} else if aggregate == utils.AGGREGATE_MAX {
					score = math.Max(item.Score, element.Score * weights[i])
				} else if aggregate == utils.AGGREGATE_MIN {
					score = math.Min(item.Score, element.Score * weights[i])
				}
			}
			ret.Add(element.Member, score, utils.UpsertPolicy, utils.SORTED_SET_UPDATE_ALL, false, false)
			return true
		})
	}
	return ret
}

func (sortedSet *SortedSet) Union(otherSet *SortedSet) *SortedSet {
	ret := Make()
	var start, stop int64
	stop = sortedSet.Len()
	sortedSet.ForEach(start, stop, false, func(element *Element) bool {
		ret.Add(element.Member, element.Score, utils.UpsertPolicy, utils.SORTED_SET_UPDATE_ALL, false, false)
		return true
	})

	stop = otherSet.Len()
	otherSet.ForEach(start, stop, false, func(element *Element) bool {
		ret.Add(element.Member, element.Score, utils.UpsertPolicy, utils.SORTED_SET_UPDATE_ALL, false, false)
		return true
	})
	return ret
}

func (sortedSet *SortedSet) Diff(otherSet *SortedSet) *SortedSet {
	ret := Make()
	var start, stop int64
	stop = sortedSet.Len()
	sortedSet.ForEach(start, stop, false, func(element *Element) bool {
		_, exists := otherSet.Get(element.Member)
		if !exists {
			ret.Add(element.Member, element.Score, utils.UpsertPolicy, utils.SORTED_SET_UPDATE_ALL, false, false)
		}
		return true
	})
	return ret
}

func (sortedSet *SortedSet) applyWeight(weight float64) *SortedSet {
	ret := Make()
	sortedSet.ForEach(0, sortedSet.Len(), false, func(element *Element) bool {
		ret.Add(element.Member, element.Score * weight, utils.UpsertPolicy, utils.SORTED_SET_UPDATE_ALL, false, false)
		return true
	})
	return ret
}

func Intersects(sets []*SortedSet, weights []float64, aggregate int) *SortedSet {
	n := len(sets)
	if n == 0 {
		return nil
	}

	setsWithWeights := make([]*SortedSet, len(sets))
	for i:=0;i<n;i++ {
		setsWithWeights[i] = sets[i].applyWeight(weights[i])
	}

	if n == 1 {
		return setsWithWeights[0]
	}
	i := 1
	ret := setsWithWeights[0].intersect(setsWithWeights[i], aggregate)

	for i=2;i<n;i++ {
		ret = ret.intersect(setsWithWeights[i], aggregate)
	}
	return ret
}

func (sortedSet *SortedSet) intersect(otherSet *SortedSet, aggregate int) *SortedSet {
	ret := Make()
	var score float64 = 0
	// 先遍历集合 A，找出 既属于 A 又属于 B 的元素放入 ret
	sortedSet.ForEach(0, sortedSet.Len(), false, func(element *Element) bool {
		if otherElement, exists := otherSet.Get(element.Member); exists {
			if aggregate == utils.AGGREGATE_SUM {
				score = element.Score + otherElement.Score
			} else if aggregate == utils.AGGREGATE_MAX {
				score = math.Max(element.Score, otherElement.Score)
			} else if aggregate == utils.AGGREGATE_MIN {
				score = math.Min(element.Score, otherElement.Score)
			}
			ret.Add(element.Member, score, utils.UpsertPolicy, utils.SORTED_SET_UPDATE_ALL, false, false)
		}
		return true
	})
	return ret
}