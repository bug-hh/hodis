package set

type Consumer func(val string) bool

type Set interface {
	Add(val string) int
	Remove(val string) bool

	Contains(val string) bool
	Len() int

	Intersect(anotherSet Set) Set
	Union(another Set) Set
	Diff(another Set) Set

	ForEach(consumer Consumer)

	RandomMembers(limit int) []string
	RandomDistinctMembers(limit int) []string
}
