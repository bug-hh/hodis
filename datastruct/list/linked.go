package list

type LinkedList struct {
	first *node
	last *node
	size int
}

type node struct {
	val interface{}
	prev *node
	next *node
}

func (list *LinkedList) Add(val interface{}) {
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

func (list LinkedList) Contains(expected Expected) bool {
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
	if list == nil {
		panic("list is nil")
	}
	return list.size
}
