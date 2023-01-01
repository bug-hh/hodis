package set

import "github.com/hodis/datastruct/dict"

// HashSet is a set of elements based on hash table
type HashSet struct {
	dict dict.Dict
}

func (s *HashSet) RandomDistinctMembers(limit int) []string {
	return s.dict.RandomDistinctKeys(limit)
}

func (s *HashSet) RandomMembers(limit int) []string {
	return s.dict.RandomKeys(limit)
}

func (s *HashSet) Add(val string) int {
	return s.dict.Put(val, nil)
}

func (s *HashSet) Remove(val string) bool {
	return s.dict.Remove(val) == 1
}

func (s *HashSet) Contains(val string) bool {
	_, exists := s.dict.Get(val)
	return exists
}

func (s *HashSet) Intersect(anotherSet Set) Set {
	ret := NewHashSet()

	anotherSet.ForEach(func(val string) bool {
		if s.Contains(val) {
			ret.Add(val)
		}
		return true
	})
	return ret
}

func (s *HashSet) Union(another Set) Set {
	ret := NewHashSet()
	another.ForEach(func(val string) bool {
		ret.Add(val)
		return true
	})

	s.ForEach(func(val string) bool {
		ret.Add(val)
		return true
	})
	return ret
}

func (s *HashSet) Diff(another Set) Set {
	ret := NewHashSet()
	s.ForEach(func(val string) bool {
		if !another.Contains(val) {
			ret.Add(val)
		}
		return true
	})
	return ret
}

func NewHashSet() *HashSet {
	return &HashSet{
		dict: dict.MakeSimple(),
	}
}

func (s *HashSet) Len() int {
	return s.dict.Len()
}

func (s *HashSet) ForEach(consumer Consumer) {
	s.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

