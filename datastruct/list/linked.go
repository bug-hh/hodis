package list

import "sync"

type LinkedList struct {
	first *node
	last *node
	size int
	mutex sync.RWMutex
}

type node struct {
	val interface{}
	prev *node
	next *node
}

func (list *LinkedList) Add(val interface{}) {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list == nil {
		panic("list is nil")
	}
	n := &node{
		val: val,
	}

	if list.last == nil {
		list.first = n
		list.last = n
	} else {
		n.prev = list.last
		list.last.next = n
		list.last = n
	}
	list.size++
}

func Make(vals ...interface{}) *LinkedList {
	list := LinkedList{}
	for _, v := range vals {
		list.Add(v)
	}
	return &list
}

func (list *LinkedList) ForEach(consumer Consumer) {
	if list == nil {
		panic("list is nil")
	}
	n := list.first
	i := 0
	for n != nil {
		goNext := consumer(i, n.val)
		if !goNext {
			break
		}
		i++
		n = n.next
	}
}

func (list *LinkedList) Contains(expected Expected) bool {
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
func (list *LinkedList) Len() int {
	list.mutex.RLock()
	defer list.mutex.RUnlock()
	if list == nil {
		panic("list is nil")
	}
	return list.size
}

func (list *LinkedList) RemoveFirst() {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	if list.first == nil || list.size == 0 {
		return
	}
	next := list.first.next
	// 有可能链表就只有一个元素
	if next != nil {
		next.prev = nil
	}

	list.first = next
	list.size--
}

func (list *LinkedList) Get(expected Expected) *node {
	if list == nil {
		panic("list is nil")
	}
	n := list.first
	for n != nil {
		if expected(n.val) {
			return n
		}
		n = n.next
	}
	return nil
}

func (list *LinkedList) Remove(n *node) {
	if n == nil {
		return
	}
	if list.size == 1 {
		list.first = nil
		list.last = nil
	}
	// 表示要移除的元素是队头
	if n.prev == nil {
		list.RemoveFirst()
		return
	} else {
		n.prev.next = n.next
		n.next.prev = n.prev
	}
	list.size--
}