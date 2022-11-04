package database

import "github.com/hodis/interface/redis"


// DB is the interface for redis style storage engine
type DB interface {
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}


type DataEntity struct {
	Data interface{}
}