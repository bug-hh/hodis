package sortedset

import (
	"math/rand"
	"strings"
)

// 跳跃表讲解：https://blog.csdn.net/vandavidchou/article/details/104099829
// redis skiplist https://blog.csdn.net/hbhgyt/article/details/123857645?spm=1001.2101.3001.6650.11&utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-11-123857645-blog-128136794.pc_relevant_recovery_v2&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-11-123857645-blog-128136794.pc_relevant_recovery_v2&utm_relevant_index=12
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
	Header *node
	Tail   *node
	Length int64
	Level  int16 // 记录跳跃表内层数最大的节点的层数
}

func (sk *skiplist) GetHeader() *node {
	return sk.Header
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

func NewSkipList() *skiplist {
	return makeSkiplist()
}

func makeSkiplist() *skiplist {
	return &skiplist{
		Level:  1,
		Header: makeNode(maxLevel, 0, ""),
	}
}

/*
 * return: has found and removed node
 */
func (sk *skiplist) remove(member string, score float64) bool {
	// update[i] 表示第 i 层链表里，最接近 score 的节点
	update := make([]*node, maxLevel)
	nd := sk.Header
	for i:=sk.Level-1;i>=0;i-- {
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
	for i:=int16(0);i<sk.Level;i++ {
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
	// 被删除节点不是最后一个节点
	if nd.level[0].forward != nil {
		nd.level[0].forward.backward = nd.backward
	} else {
		sk.Tail = nd.backward
	}

	for sk.Level > 1 && sk.Header.level[sk.Level-1].forward == nil {
		sk.Level--
	}
	sk.Length--
}

func judge(level *Level, score float64, member string) bool {
	if level != nil && level.forward != nil {
		return level.forward.Score < score ||
			(level.forward.Score == score &&
				strings.Compare(level.forward.Member, member) < 0)
	}
	return false
}
func (sk *skiplist) insert(member string, score float64) *node {
	// update[i] 代表的是第 i 层链表里，最接近 score 的节点
	update := make([]*node, maxLevel)
	/* rank[i] 表示第 i 层链表里，最接近 score 的节点（假设叫节点 A）的跨度，
	也就是，第 i 层链表的表头到节点 A 之间的节点数量
	 */
	rank := make([]int64, maxLevel)

	nd := sk.Header
	for i:=sk.Level -1;i>=0;i-- {
		if i == sk.Level-1 {
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
	if level > sk.Level {
		for i := sk.Level;i<level;i++ {
			rank[i] = 0
			update[i] = sk.Header
			update[i].level[i].span = sk.Length
		}
		sk.Level = level
	}

	/*
	代码执行到这，nd 表示要插入的结点，update 表示 nd 的前驱结点
	 */
	nd = makeNode(level, score, member)
	// 对 [0,Level) 层，每层都插入一个新的结点
	for i:=int16(0);i<level;i++ {
		nd.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = nd

		// 更新 span
		nd.level[i].span = update[i].level[i].span - (rank[0]-rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 对于 [Level, sk.Level) 只增加 span
	for i:=level;i<sk.Level;i++ {
		update[i].level[i].span++
	}

	// 设置 backword
	// 如果 update 是表头
	if update[0] == sk.Header {
		nd.backward = nil
	} else {
		nd.backward = update[0]
	}

	if nd.level[0].forward != nil {
		nd.level[0].forward.backward = nd
	} else {
		sk.Tail = nd
	}
	sk.Length++
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
	n := skiplist.Header
	// scan from top Level
	for level := skiplist.Level - 1; level >= 0; level-- {
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