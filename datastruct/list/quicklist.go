package list

import (
	"container/list"
)

// pageSize must be even
const pageSize = 1024

type QuickList struct {
	data *list.List
}

// 返回给定位置的 element
func (ql *QuickList) move(index int) *list.Element {
	if ql.data == nil || ql.data.Len() == 0 {
		return nil
	}
	currentNode := ql.data.Front()
	for i:=0;i<index && currentNode != nil;i++ {
		currentNode = currentNode.Next()
	}
	return currentNode
}

func (ql *QuickList) Get(index int) (val interface{}) {
	currentNode := ql.move(index)
	if currentNode == nil {
		return nil
	}
	return currentNode.Value
}

func (ql *QuickList) Set(index int, val interface{}) {
	currentNode := ql.move(index)
	if currentNode == nil {
		return
	}
	currentNode.Value = val
}

func (ql *QuickList) Insert(index int, val interface{}) {
	currentNode := ql.move(index)
	if currentNode == nil {
		ql.data.PushBack(val)
		return
	}
	ql.data.InsertBefore(val, currentNode)
}

func (ql *QuickList) Remove(index int) (val interface{}) {
	currentNode := ql.move(index)
	if currentNode != nil {
		ql.data.Remove(currentNode)
	}
	return currentNode.Value
}

func (ql *QuickList) RemoveLast() interface{} {
	if ql.data == nil || ql.data.Len() == 0 {
		return nil
	}

	return ql.data.Remove(ql.data.Back())
}

func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	if ql.data == nil {
		return 0
	}
	ret := 0
	currentNode := ql.data.Front()

	for currentNode != nil {
		if expected(currentNode.Value) {
			next := currentNode.Next()
			ql.data.Remove(currentNode)
			ret++
			currentNode = next
		} else {
			currentNode = currentNode.Next()
		}
	}
	return ret
}

func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	var currentNode *list.Element
	ret := 0
	if count < 0 {
		count *= -1
		currentNode = ql.data.Back()
		for currentNode != nil && count > 0 {
			if expected(currentNode.Value) {
				prev := currentNode.Prev()
				ql.data.Remove(currentNode)
				currentNode = prev
				count--
				ret++
				if count == 0 {
					break
				}
			} else {
				currentNode = currentNode.Prev()
			}
		}
	} else {
		currentNode = ql.data.Front()
		for currentNode != nil && count > 0 {
			if expected(currentNode.Value) {
				next := currentNode.Next()
				ql.data.Remove(currentNode)
				currentNode = next
				count--
				ret++
				if count == 0 {
					break
				}
			} else {
				currentNode = currentNode.Next()
			}
		}
	}
	return ret
}

func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	return ql.RemoveByVal(expected, count)
}

func (ql *QuickList) Len() int {
	return ql.data.Len()
}

func (ql *QuickList) ForEach(consumer Consumer) {
	if ql.data == nil {
		return
	}
	currentNode := ql.data.Front()
	i := 0
	for currentNode != nil {
		if !consumer(i, currentNode.Value) {
			return
		}
		currentNode = currentNode.Next()
	}
}

func (ql *QuickList) Contains(expected Expected) bool {
	if ql.data == nil {
		return false
	}
	currentNode := ql.data.Front()
	for currentNode != nil {
		if expected(currentNode.Value) {
			return true
		}
		currentNode = currentNode.Next()
	}
	return false
}

func (ql *QuickList) Range(start int, stop int) []interface{} {
	if start < 0 || start >= ql.Len() || stop < start || stop > ql.Len() {
		return nil
	}

	startNode := ql.move(start)
	stopNode := ql.move(stop)

	ret := make([]interface{}, 0, stop-start)
	for startNode != nil && startNode != stopNode {
		ret = append(ret, startNode.Value)
		startNode = startNode.Next()
	}
	return ret
}

func NewQuickList() *QuickList {
	ret := &QuickList{
		data: list.New(),
	}
	return ret
}

func (ql *QuickList) IndexOfVal(expected Expected) (index int) {
	if ql.data == nil || ql.data.Len() == 0 {
		return -1
	}

	currentNode := ql.data.Front()
	i := 0
	for currentNode != nil {
		if expected(currentNode.Value) {
			return i
		}
		i++
		currentNode = currentNode.Next()
	}
	return -1
}

func (ql *QuickList) Add(val interface{}) {
	ql.data.PushBack(val)
}

func (ql *QuickList) Trim(start int, stop int) {
	if ql.data == nil || ql.data.Len() == 0 {
		return
	}
	startNode := ql.move(start)
	stopNode := ql.move(stop)

	if startNode != nil && startNode.Prev() != nil {
		ql.data.Remove(startNode.Prev())
	}

	if stopNode != nil && stopNode.Next() != nil {
		ql.data.Remove(stopNode.Next())
	}
}