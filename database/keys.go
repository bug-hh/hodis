package database

import (
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/protocol"
	"strconv"
	"time"
)

func toTTLCmd(db *DB, key string) *protocol.MultiBulkReply {
	raw, exists := db.ttlMap.Get(key)
	if !exists {
		// has no TTL
		return protocol.MakeMultiBulkReply(utils.ToCmdLine("PERSIST", key))
	}
	expireTime, _ := raw.(time.Time)
	timestamp := strconv.FormatInt(expireTime.UnixNano()/1000/1000, 10)
	return protocol.MakeMultiBulkReply(utils.ToCmdLine("PEXPIREAT", key, timestamp))
}

func Del(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	if deleted > 0 {
		cmdLine := utils.ToCmdLine3("del", args...)
		db.addAof(cmdLine)
		syncErr := db.cmdSync(cmdLine)
		if syncErr != nil {
			logger.Warn("sync del command error: ", syncErr.Error())
		}
	}
	return protocol.MakeIntReply(int64(deleted))
}

func init() {
	RegisterCommand("Del", Del, writeAllKeys, nil, 2, flagWrite)

}