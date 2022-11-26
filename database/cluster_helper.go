package database

import (
	"github.com/hodis/interface/redis"
	"github.com/hodis/redis/protocol"
)

/*
检查传入的 keys 里，哪些 key 是存在的，返回那些存在的 key
custom command for MSetNX tcc transaction
 */
func execExistIn(db *DB, args [][]byte) redis.Reply {
	var result [][]byte
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result = append(result, []byte(key))
		}
	}
	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommand("ExistIn", execExistIn, readAllKeys, nil, -1, flagReadOnly)
}