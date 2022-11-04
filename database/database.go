package database

import (
	"fmt"
	"github.com/hodis/config"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/redis/protocol"
	"runtime/debug"
	"strings"
	"sync/atomic"
)

// MultiDB is a set of multiple database set
type MultiDB struct {
	dbSet []*atomic.Value

	// todo 需要时再实现 handle publish/subscribe
	//hub *pubsub.Hub
	// todo 需要时再实现 handle aof persistence
	//aofHandler *aof.Handler

	// store master node address
	slaveOf     string
	role int32
	// todo 需要时再实现
	//replication *replicationStatus
}

func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{}
	if config.Properties.Databases == 0 {
		// 默认 16 个数据库
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := MakeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		mdb.dbSet[i] = holder
	}

	// todo 以后实现订阅，发布功能

	// todo 以后实现 aof 功能

	// todo 以后实现 rdb 功能

	// todo 以后实现 复制 功能
	return mdb
}

// 实现 DB 接口里的所有方法，从而实现 redis 风格的数据库存储引擎
func (mdb *MultiDB) Exec(c redis.Connection, cmdLine [][]byte)  (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	logger.Info("MultiDB cmdName: ", cmdName)
	// todo 之后实现 authenticate
	if cmdName == "auth" {

	}

	// todo 之后实现 slave


	// todo 特殊命令，以后实现
	// subscribe, publish, save, copy

	// 常规命令
	dbIndex := c.GetDBIndex()
	selectDB, errReply := mdb.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}

	return selectDB.Exec(c, cmdLine)
}


func (mdb *MultiDB) selectDB(dbIndex int) (*DB, *protocol.StandardErrReply) {
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR DB Index is out of range")
	}

	return mdb.dbSet[dbIndex].Load().(*DB), nil
}


// AfterClientClose does some clean after client close connection
func (mdb *MultiDB) AfterClientClose(c redis.Connection) {

}

// Close graceful shutdown database
func (mdb *MultiDB) Close() {
	// stop replication first

}


