package set

import "github.com/hodis/datastruct/dict"

// Set is a set of elements based on hash table
type Set struct {
	dict dict.Dict
}

func (s *Set) Len() int {
	return s.dict.Len()
}

func (s *Set) ForEach(consumer func(member string) bool) {
	s.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}