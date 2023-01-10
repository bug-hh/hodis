package database

import (
	"github.com/hodis/interface/redis"
	"github.com/hodis/redis/protocol"
)

func Ping(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

func init() {
	RegisterCommand("Ping", Ping, noPrepare, nil, -1, flagReadOnly)
}