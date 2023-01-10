package utils

const (
	SORTED_SET_UPDATE_ALL = iota
	SORTED_SET_UPDATE_GREATER
	SORTED_SET_UPDATE_LESS_THAN
)

const (
	UpsertPolicy = iota // default
	InsertPolicy        // set nx
	UpdatePolicy        // set ex  or xx
)

const (
	AGGREGATE_SUM = iota
	AGGREGATE_MIN
	AGGREGATE_MAX
)