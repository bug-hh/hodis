package sortedset

import "strconv"

// SortedSet is a set which keys sorted by bound score
type SortedSet struct {
	dict     map[string]*Element
	skiplist *skiplist
}

// Len returns number of members in set
func (sortedSet *SortedSet) Len() int64 {
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


