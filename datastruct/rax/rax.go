package rax

import (
	"container/list"
	"fmt"
	"sort"
)

// https://zhouyx.blog.csdn.net/article/details/122029659?spm=1001.2101.3001.6650.4&utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-4-122029659-blog-99322712.pc_relevant_3mothn_strategy_and_data_recovery&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-4-122029659-blog-99322712.pc_relevant_3mothn_strategy_and_data_recovery&utm_relevant_index=5
/*
如果是非压缩节点，有多个字符，对应多个节点，需要对所有字符进行排序
如果是压缩节点，只有一个节点，无需排序
 */
type RaxNode struct {
	isKey bool
	isComp bool
	isNull bool
	size int
	childChars []byte
	childrenMap map[string]*RaxNode
	data interface{}
}

type Rax struct {
	Head *RaxNode
	NumEle int64
	NumNodes int64
}

func NewRax() *Rax {
	// 直接使用默认零值
	h := NewRaxNode()
	return &Rax{
		Head:     h,
		NumEle:   0,
		NumNodes: 0,
	}
}

func NewRaxNode() *RaxNode {
	return &RaxNode{
		childrenMap: make(map[string]*RaxNode),
	}
}

func RaxCopy(dst *RaxNode, src *RaxNode) {
	dst.isKey = src.isKey
	dst.isComp = src.isComp
	dst.isNull = src.isNull
	dst.size = src.size

	for k, v := range src.childrenMap {
		dst.childrenMap[k] = v
	}
	dst.data = src.data
}

func RaxLowWalk(rax *Rax, key *string, stopNode **RaxNode, prevNode **RaxNode, splitPos *int) int {
	cur := rax.Head
	i, j := 0, 0
	length := len(*key)
	var chars string
	for cur.size > 0 && i < length {
		if cur.isComp {
			for k, _ := range cur.childrenMap {
				chars = k
				break
			}
			for j=0;j < cur.size && i < length;j++ {
				if chars[j] != (*key)[i] {
					break
				}
				i++
			}
			/*
			代码执行到这，如果 j == cur.size, 表示 key[0:i] 与 cur 内的字符完全匹配
			这时分两种情况：
			1、i == length, 表示 key 完全匹配，cur 会调整为它的子节点后，再退出整个循环，此时 stopnode = cur
			2、i < length, 表示 key 没有匹配完，但是因为 j == cur.size, 表示当前层级内的字符匹配完，cur 需要跳转到下一层级后，继续和 key 比较
			 */

			/*
			如果 j != cur.size, 表示 key[i] 与 cur.chars[j] 出现了不匹配的情况，也就是找到了分割点，所以直接退出整个循环
			 */
			if j != cur.size {
				break
			}
			cur = cur.childrenMap[chars]
		} else {
			var misMatchKey string
			var misMatchIndex = 0
			var res = 0
			var isMiss bool
			// cur 是一个非压缩节点, 这里循环遍历的每个 k 都是一条子路径
			for k, _ := range cur.childrenMap {
				res, isMiss = findMismatchIndex(k, *key, i)
				// 如果为 -1，表示 k 能和 key 中字符匹配上（可能是部分匹配，也可能是完全匹配）
				if !isMiss {
					misMatchIndex = -1
					misMatchKey = k
					i = res
					break
				}
				// 如果不是完全匹配，那么找到尽可能多的前缀匹配对应的子路径, 并记录下这条子路径
				if misMatchIndex <= res {
					misMatchIndex = res
					misMatchKey = k
				}
			}
			// key 和 cur 的所有子节点都不匹配，所以应该给 cur 增加一个子节点，来存放当前的key，所以 stopNode = cur
			if isMiss {
				// 找到 misMatchIndex, 将 cur 调整到对应的子节点上，i = misMatchIndex, 返回
				//cur = cur.childrenMap[misMatchKey]
				*stopNode = cur
				i = misMatchIndex
				return i
			} else {
				// 表示当前子路径能和 key 中字符匹配上（可能是部分匹配，也可能是完全匹配）
				// 还是要判断 key 有没有比完，如果没有比完，那么 cur 要再次跳转到下一个子节点
				// 没比完
				if i < length {
					cur = cur.childrenMap[misMatchKey]
				} else {
					// 如果 key 已经比完，那么 cur 跳转到对应子路径，*stopNode = cur, 然后 i = length, return i
					cur = cur.childrenMap[misMatchKey]
					*stopNode = cur
					i = length
					return i
				}
			}
		}
		j = 0
	}
	*stopNode = cur
	if splitPos != nil && cur.isComp {
		*splitPos = j
	}
	return i
}

// key[start] 开始比
func findMismatchIndex(s1 string, key string, start int) (int, bool) {
	length := myMin(len(s1), len(key))
	i := 0
	for i=0;i<length;i++ {
		if s1[i] != key[start] {
			return start, true
		}
		start++
	}
	return start, false
}

func myMax(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func myMin(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func RaxInsert(rax *Rax, key *string, value interface{}) int {
	var stopNode, prevNode *RaxNode
	splitPos := 0
	i := RaxLowWalk(rax, key, &stopNode, &prevNode, &splitPos)
	length := len(*key)

	/* 分四种情况
	1、i == len && !stopNode->iscompr
	2、i == len && stopNode->iscompr
	3、i != len && !stopNode->iscompr
	4、i != len && stopNode->iscompr
	 */

	// 已存在 abcd，再插入 abcd
	if i == length && !stopNode.isComp {
		if !stopNode.isKey {
			stopNode.isKey = true
		}

		stopNode.data = value
		stopNode.isNull = false

		return 0
	}

	// 已经存在 abcd，插入 ab
	if i == length && stopNode.isComp {
		// 保存原来 abcd 的子节点
		var next *RaxNode
		var chars string
		for k, node := range stopNode.childrenMap {
			chars = k
			next = node
			break
		}

		stopNode.childrenMap = make(map[string]*RaxNode)

		suffix := chars[splitPos:]
		prefix := chars[:splitPos]

		suffixNode := NewRaxNode()
		suffixNode.isKey = true
		suffixNode.isComp = true
		suffixNode.isNull = false
		suffixNode.size = len(suffix)
		suffixNode.data = value
		suffixNode.childrenMap[suffix] = next

		stopNode.childrenMap[prefix] = suffixNode
		stopNode.size = len(prefix)

		rax.NumEle++
		return 1
	}

	if i != length && !stopNode.isComp {
		/*
		例如，已存在 a，插入 akb 或者是首次插入
		 */
		suffix := (*key)[i:]
		if stopNode.size == 0 && length-i > 1 {
			// 生成并插入压缩节点
			newNode := NewRaxNode()
			newNode.isKey = true
			newNode.data = value
			stopNode.isNull = true
			stopNode.isComp = true
			stopNode.childrenMap[suffix] = newNode
			stopNode.size = len(suffix)
		} else {
			// stop node 是非压缩节点, 在 stop node 的 children 中新增一个节点
			newNode := NewRaxNode()
			sl := len(suffix)
			//for m:=0;m<sl;m++ {
			//	k := string(suffix[m])
			//	newNode.childrenMap[k] = NewRaxNode()
			//	newNode.size = len(newNode.childrenMap)
			//	if m == sl-1 {
			//		newNode.childrenMap[k].isKey = true
			//		newNode.childrenMap[k].data = value
			//	}
			//	newNode = newNode.childrenMap[k]
			//}
			if sl > 1 {
				sf := suffix[1:]
				newNode.isComp = len(sf) > 1
				newNode.isNull = true
				newNode.childrenMap[sf] = NewRaxNode()
				newNode.childrenMap[sf].isKey = true
				newNode.childrenMap[sf].data = value
				if newNode.isComp {
					newNode.size = len(sf)
				} else {
					newNode.size = len(newNode.childrenMap)
				}
			}
			stopNode.childrenMap[string(suffix[0])] = newNode
			stopNode.size = len(stopNode.childrenMap)
		}

		rax.NumEle++
		return 1
	}

	// 例如已存在 XYWZ, 插入 ABCD
	if i != length && stopNode.isComp {
		//splitChar := (*key)[splitPos]

		var stopChars string
		var oldValue interface{}
		var oldStopNode *RaxNode

		for k, oldNode := range stopNode.childrenMap {
			stopChars = k
			oldStopNode = oldNode
			if oldNode.isKey && !oldNode.isNull {
				oldValue = oldNode.data
			}
			break
		}

		// 从 splitPos 开始，与 key 不匹配，即：在 splitPos 之前都是匹配的
		// 所以肯定有 stopPrefix == newNodePrefix

		stopPrefix := stopChars[:splitPos]
		stopSuffix := stopChars[splitPos:]

		//newNodePrefix := (*key)[:splitPos]
		newNodeSuffix := (*key)[splitPos:]

		/*
		因为需要生成新的树形结构，所以直接重置 map
		 */
		stopNode.childrenMap = make(map[string]*RaxNode)

		/*
		middleNode 是一个非压缩节点，但是非压缩节点中，只能用单个字符来代表某个子节点
		 */

		var middleNode, stopSuffixNode, newSuffixNode *RaxNode
		middleNode = NewRaxNode()
		newSuffixNode = NewRaxNode()

		if oldStopNode != nil && oldStopNode.childrenMap != nil && len(oldStopNode.childrenMap) > 0 {
			stopSuffixNode = oldStopNode
		} else {
			stopSuffixNode = NewRaxNode()
			if len(stopSuffix) > 1 {
				ss := stopSuffix[1:]
				stopSuffixNode.childrenMap[ss] = NewRaxNode()
				stopSuffixNode.isComp = len(ss) > 1
				stopSuffixNode.childrenMap[ss].isKey = true
				stopSuffixNode.childrenMap[ss].data = oldValue
				if stopSuffixNode.isComp {
					stopSuffixNode.size = len(ss)
				} else {
					stopSuffixNode.size = len(stopSuffixNode.childrenMap)
				}
			} else {
				stopSuffixNode.isKey = true
				stopSuffixNode.data = oldValue
			}
		}

		if len(newNodeSuffix) > 1 {
			ns := newNodeSuffix[1:]
			newSuffixNode.childrenMap[ns] = NewRaxNode()
			newSuffixNode.isComp = len(ns) > 1
			newSuffixNode.childrenMap[ns].isKey = true
			newSuffixNode.childrenMap[ns].data = value
			if newSuffixNode.isComp {
				newSuffixNode.size = len(ns)
			} else {
				newSuffixNode.size = len(newSuffixNode.childrenMap)
			}
		} else {
			newSuffixNode.isKey = true
			newSuffixNode.data = value
		}

		middleNode.childrenMap[string(stopSuffix[0])] = stopSuffixNode
		middleNode.childrenMap[string(newNodeSuffix[0])] = newSuffixNode
		middleNode.size = len(middleNode.childrenMap)

		if len(stopPrefix) == 0 {
			RaxCopy(stopNode, middleNode)
		} else {
			stopNode.childrenMap[stopPrefix] = middleNode
			stopNode.isComp = true
			stopNode.size = len(stopPrefix)
		}
		rax.NumEle++
	}
	return 0
}


func RaxShow(rax *Rax) {
	raxShow(rax.Head, 0)
}

func raxShow(rax *RaxNode, level int) {
	ls := list.New()
	if rax == nil {
		return
	}
	ls.PushBack(rax)
	for ls.Len() > 0 {
		node := ls.Front().Value.(*RaxNode)
		ls.Remove(ls.Front())

		if node.isComp {
			for k, v := range node.childrenMap {
				fmt.Printf("%s", k)
				ls.PushBack(v)
			}
		} else {
			chars := make([]string, 0, len(node.childrenMap))
			for k, _ := range node.childrenMap {
				chars = append(chars, k)
			}
			sort.Strings(chars)
			for _, ch := range chars {
				fmt.Printf("%s", ch)
				ls.PushBack(node.childrenMap[ch])
			}
		}

		if len(node.childrenMap) > 0 {
			fmt.Printf("\n")
		}

	}
}