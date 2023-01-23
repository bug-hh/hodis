package pubsub

import (
	"github.com/hodis/datastruct/dict"
	"github.com/hodis/datastruct/lock"
)

// Hub stores all subscribe relations
type Hub struct {
	// channel -> list(*Client)
	subs dict.Dict
	// pattern -> list(*Client)
	patternSubs dict.Dict
	// lock channel
	subsLocker *lock.Locks
}

// MakeHub creates new hub
func MakeHub() *Hub {
	return &Hub{
		subs:       dict.MakeConcurrent(4),
		patternSubs: dict.MakeConcurrent(4),
		subsLocker: lock.Make(16),
	}
}
