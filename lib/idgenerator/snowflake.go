package idgenerator

import (
	"hash/fnv"
	"sync"
	"time"
)

/*
这里用到了 twitter 的 snowflake 64 位自增 id 算法
 */
const (
	// epoch0 is set to the twitter snowflake epoch of Nov 04 2010 01:42:54 UTC in milliseconds
	// You may customize this to set a different epoch for your application.
	epoch0      int64 = 1288834974657
	maxSequence int64 = -1 ^ (-1 << uint64(nodeLeft))
	timeLeft    uint8 = 22
	nodeLeft    uint8 = 10
	nodeMask    int64 = -1 ^ (-1 << uint64(timeLeft-nodeLeft))
)

type IDGenerator struct {
	mu *sync.Mutex
	lastStamp int64
	nodeID int64
	sequence int64
	epoch time.Time
}

func MakeGenerator(node string) *IDGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(node))
	nodeID := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	epoch := curTime.Add(time.Unix(epoch0/1000, (epoch0%1000)*1000000).Sub(curTime))

	return &IDGenerator{
		mu:        &sync.Mutex{},
		lastStamp: -1,
		nodeID:    nodeID,
		sequence:  1,
		epoch:     epoch,
	}
}