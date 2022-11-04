package sortedset

const (
	maxLevel = 16
)

// Element is a key-score pair
type Element struct {
	Member string
	Score  float64
}

// Level aspect of a node
type Level struct {
	forward *node // forward node has greater score
	span    int64
}

type node struct {
	Element
	backward *node
	level    []*Level // level[0] is base level
}

type skiplist struct {
	header *node
	tail   *node
	length int64
	level  int16
}

/*
 * 1-based rank
 */
func (skiplist *skiplist) getByRank(rank int64) *node {
	var i int64 = 0
	n := skiplist.header
	// scan from top level
	for level := skiplist.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}