package list

import "container/list"

// pageSize must be even
const pageSize = 1024

type QuickList struct {
	data *list.List
	size int
}

func (ql *QuickList) Get(index int) (val interface{}) {
	iter := ql.find(index)
	return iter.get()
}

func (ql *QuickList) Set(index int, val interface{}) {
	iter := ql.find(index)
	iter.set(val)
}

func (ql *QuickList) Insert(index int, val interface{}) {
	if index == ql.size {
		ql.Add(val)
		return
	}
	iter := ql.find(index)
	page := iter.page()
	if len(page) < pageSize {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
		iter.node.Value = page
		ql.size++
		return
	}

	var nextPage []interface{}
	nextPage = append(nextPage, page[pageSize/2:]...)
	page = page[:pageSize/2]
	if iter.offset < len(page) {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
	} else {
		i := iter.offset-pageSize/2
		nextPage = append(nextPage[:i+1], nextPage[i:]...)
		nextPage[i] = val
	}
	iter.node.Value = page
	ql.data.InsertAfter(nextPage, iter.node)
	ql.size++
}

func (ql *QuickList) Remove(index int) (val interface{}) {
	iter := ql.find(index)
	return iter.remove()
}

func (ql *QuickList) RemoveLast() interface{} {
	if ql.Len() == 0 {
		return nil
	}

	ql.size--
	lastNode := ql.data.Back()
	lastPage := lastNode.Value.([]interface{})
	if len(lastPage) == 1 {
		ql.data.Remove(lastNode)
		return lastPage[0]
	}
	val := lastPage[len(lastPage)-1]
	lastPage = lastPage[0:len(lastPage)-1]
	lastNode.Value = lastPage
	return val
}

func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	panic("implement me")
}

func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	panic("implement me")
}

func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	panic("implement me")
}

func (ql *QuickList) Len() int {
	return ql.size
}

func (ql *QuickList) ForEach(consumer Consumer) {
	panic("implement me")
}

func (ql *QuickList) Contains(expected Expected) bool {
	panic("implement me")
}

func (ql *QuickList) Range(start int, stop int) []interface{} {
	if start < 0 || start >= ql.Len() {
		panic("`start` out of range")
	}

	if stop < start || stop > ql.Len() {
		panic("`stop` out of range")
	}

	sliceSize := stop - start
	slice := make([]interface{}, 0 ,sliceSize)

	iter := ql.find(start)
	i := 0
	for i < sliceSize {
		slice = append(slice, iter.get())
		iter.next()
		i++
	}
	return slice
}

type iterator struct {
	node *list.Element
	offset int
	ql *QuickList
}

func NewQuickList() *QuickList {
	ret := &QuickList{
		data: list.New(),
	}
	return ret
}

func (ql *QuickList) Add(val interface{}) {
	ql.size++
	if ql.data.Len() == 0 {
		page := make([]interface{}, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	backNode := ql.data.Back()
	backPage := backNode.Value.([]interface{})
	if len(backPage) == cap(backPage) {
		page := make([]interface{}, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}

	backPage = append(backPage, val)
	backNode.Value = backPage
}

func (ql *QuickList) find(index int) *iterator {
	if ql == nil {
		panic("list is nil")
	}

	if index < 0 || index >= ql.size {
		panic("index out of bound")
	}

	var n *list.Element
	var page []interface{}

	var pageBeg int
	if index < ql.size / 2 {
		// 从头开始搜索
		n = ql.data.Front()
		pageBeg = 0
		for {
			page = n.Value.([]interface{})
			/*
			list 的每个元素是一个 page，而一个 page 又是一个 []interface{},
			page 的大小为 1024，
			pageBeg + len(page) > index 表示要找的元素在当前 page 里
			 */
			if pageBeg + len(page) > index {
				break
			}
			pageBeg += len(page)
			n = n.Next()
		}
	} else {
		// 从后半部开始找
		n = ql.data.Back()
		pageBeg = ql.size
		for {
			page = n.Value.([]interface{})
			pageBeg -= len(page)
			if pageBeg <= index {
				break
			}
			n = n.Prev()
		}
	}

	pageOffset := index - pageBeg

	/*
	返回 index 所在的 page 对应的 node（page)，以及在 page 内的索引
	 */
	return &iterator{
		node: n,
		offset: pageOffset,
		ql: ql,
	}
}

func (iter *iterator) set(val interface{}) {
	page := iter.page()
	page[iter.offset] = val
}

func (iter *iterator) get() interface{} {
	return iter.page()[iter.offset]
}

func (iter *iterator) page() []interface{} {
	return iter.node.Value.([]interface{})
}

func (iter *iterator) next() bool {
	page := iter.page()
	if iter.offset < len(page)-1 {
		iter.offset++
		return true
	}
	if iter.node == iter.ql.data.Back() {
		iter.offset = len(page)
		return false
	}
	iter.offset = 0
	iter.node = iter.node.Next()
	return true
}

func (iter *iterator) prev() bool {
	if iter.offset > 0 {
		iter.offset--
		return true
	}
	if iter.node == iter.ql.data.Front() {
		iter.offset = -1
		return false
	}
	iter.node = iter.node.Prev()
	prevPage := iter.node.Value.([]interface{})
	iter.offset = len(prevPage)-1
	return true
}

func (iter *iterator) remove() interface{} {
	page := iter.page()
	val := page[iter.offset]
	// 删除 iter.offset 位置处的元素
	page = append(page[:iter.offset], page[iter.offset+1:]...)
	// 删除后，page 不为空了
	if len(page) > 0 {
		iter.node.Value = page
		if iter.offset == len(page) {
			if iter.node != iter.ql.data.Back() {
				iter.node = iter.node.Next()
				iter.offset = 0
			}
		}
	} else {
		if iter.node == iter.ql.data.Back() {
			iter.ql.data.Remove(iter.node)
			iter.node = nil
			iter.offset = 0
		} else {
			nextNode := iter.node.Next()
			iter.ql.data.Remove(iter.node)
			iter.node = nextNode
			iter.offset = 0
		}
	}

	iter.ql.size--
	return val
}

