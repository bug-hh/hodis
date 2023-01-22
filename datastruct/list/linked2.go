package list

import (
	"container/list"
	"sync"
)

type LinkedList2 struct {
	ls *list.List
	mutex sync.RWMutex
}

func NewLinkedList() *LinkedList2 {
	return &LinkedList2{
		ls: list.New(),
	}
}

func (list *LinkedList2) Add(val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	list.mutex.Lock()
	defer list.mutex.Unlock()
	list.ls.PushBack(val)
}

func MakeFromVals(vals ...interface{}) *LinkedList2 {
	ret := NewLinkedList()
	for _, v := range vals {
		ret.Add(v)
	}
	return ret
}

func (list *LinkedList2) ForEach(consumer Consumer) {
	if list == nil {
		panic("list is nil")
	}
	n := list.ls.Front()
	i := 0
	for n != nil {
		goNext := consumer(i, n.Value)
		if !goNext {
			break
		}
		i++
		n = n.Next()
	}
}

func (list *LinkedList2) Contains(expected Expected) bool {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	contains := false
	list.ForEach(func(i int, v interface{}) bool {
		if expected(v) {
			contains = true
			return false
		}
		return true
	})
	return contains
}

// Len returns the number of elements in list
func (list *LinkedList2) Len() int {
	list.mutex.RLock()
	defer list.mutex.RUnlock()
	if list == nil {
		panic("list is nil")
	}
	return list.ls.Len()
}

func (list *LinkedList2) RemoveFirst() {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	if list == nil || list.ls.Front() == nil || list.Len() == 0 {
		return
	}
	list.ls.Remove(list.ls.Front())
}