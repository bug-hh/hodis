package sortedset

import "math/rand"

// 跳跃表讲解：https://blog.csdn.net/vandavidchou/article/details/104099829
const (
	maxLevel = 16
)

// Element is a key-score pair
type Element struct {
	Member string
	Score  float64
}

func (l *Level) GetForward() *node {
	return l.forward
}

// Level aspect of a node
type Level struct {
	forward *node // forward node has greater score
	span    int64
}

func (nd *node) GetLevel() []*Level {
	return nd.level
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
	level  int16  // 记录跳跃表内层数最大的节点的层数
}

func (sk *skiplist) GetHeader() *node {
	return sk.header
}

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Member: member,
			Score: score,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		level: 1,
		header: makeNode(maxLevel, 0, ""),
	}
}

/*
 * return: has found and removed node
 */
func (sk *skiplist) remove(member string, score float64) bool {
	// update 记录的是要被删除的节点的前一个节点
	update := make([]*node, maxLevel)
	nd := sk.header
	for i:=sk.level-1;i>=0;i-- {
		for nd.level[i].forward != nil &&
			(nd.level[i].forward.Score < score ||
				(nd.level[i].forward.Score == score &&
					nd.level[i].forward.Member < member)) {
			nd = nd.level[i].forward
		}
		update[i] = nd
	}
	// 现在 nd 指向的是要被删除的节点
	nd = nd.level[0].forward
	if nd != nil && score == nd.Score && nd.Member == member {
		sk.removeNode(nd, update)
		return true
	}
	return false
}

func (sk *skiplist) removeNode(nd *node, update []*node) {
	// 删除 nd 节点，逐层删除
	for i:=int16(0);i<sk.level;i++ {
		if update[i].level[i].forward == nd {
			/*
			因为 nd 是要被删除的节点，而 update 是 nd 的前一个节点
			所以 update 的 span += nd.span-1
			 */
			update[i].level[i].span += nd.level[i].span-1
			// 删除 nd 节点，逐层删除
			update[i].level[i].forward = nd.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	// 被删除节点的最低层不为空,也就是说，被删除节点不是最后一个节点
	if nd.level[0].forward != nil {
		nd.level[0].forward.backward = nd.backward
	} else {
		sk.tail = nd.backward
	}

	for sk.level > 1 && sk.header.level[sk.level-1].forward == nil {
		sk.level--
	}
	sk.length--
}

func (sk *skiplist) insert(member string, score float64) *node {
	update := make([]*node, maxLevel)
	// 这个 rank 什么意思？
	rank := make([]int64, maxLevel)

	nd := sk.header
	for i:=sk.level-1;i>=0;i-- {
		if i == sk.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if nd.level[i] != nil {
			for nd.level[i].forward != nil &&
				(nd.level[i].forward.Score < score ||
					(nd.level[i].forward.Score == score && nd.level[i].forward.Member < member)) {
				rank[i] += nd.level[i].span
				nd = nd.level[i].forward
			}
		}
		update[i] = nd
	}

	level := randomLevel()
	if level > sk.level {
		for i := sk.level;i<level;i++ {
			rank[i] = 0
			update[i] = sk.header
			update[i].level[i].span = sk.length
		}
		sk.level = level
	}

	/*
	代码执行到这，nd 表示要插入的结点，update 表示 nd 的前驱结点
	 */
	nd = makeNode(level, score, member)
	// 对 [0,level) 层，每层都插入一个新的结点
	for i:=int16(0);i<level;i++ {
		nd.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = nd

		// 更新 span
		nd.level[i].span = update[i].level[i].span - (rank[0]-rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 对于 [level, sk.level) 只增加 span
	for i:=level;i<sk.level;i++ {
		update[i].level[i].span++
	}

	// 设置 backword
	// 如果 update 是表头
	if update[0] == sk.header {
		nd.backward = nil
	} else {
		nd.backward = update[0]
	}

	if nd.level[0].forward != nil {
		nd.level[0].forward.backward = nd
	} else {
		sk.tail = nd
	}
	sk.length++
	return nd
}

func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}
	return maxLevel
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