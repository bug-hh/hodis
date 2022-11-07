package database

import (
	"fmt"
	"github.com/hodis/aof"
	"github.com/hodis/config"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/redis/protocol"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

// MultiDB is a set of multiple database set
type MultiDB struct {
	dbSet []*atomic.Value

	// todo 需要时再实现 handle publish/subscribe
	//hub *pubsub.Hub
	// todo 需要时再实现 handle aof persistence
	aofHandler *aof.Handler

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
	validAof := false
	if config.Properties.AppendOnly {
		/*
		在 NewAOFHandler 里，
		先读取已有的 aof 文件，然后开启子协程，从 channel 里读主协程写入的命令，
		然后往 aof 文件里写入读到的命令
		 */
		aofHandler, err := aof.NewAOFHandler(mdb, func() database.EmbedDB {
			return MakeBasicMultiDB()
		})

		if err != nil {
			panic(err)
		}

		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			singleDB := db.Load().(*DB)
			singleDB.addAof = func(line CmdLine) {
				// 主协程往 aofChan 里写命令
				mdb.aofHandler.AddAof(singleDB.index, line)
			}
		}
		validAof = true
	}

	// todo 以后实现 rdb 功能
	if !validAof {}

	// todo 以后实现 复制 功能
	return mdb
}

func MakeBasicMultiDB() *MultiDB {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		holder := &atomic.Value{}
		holder.Store(makeBasicDB())
		mdb.dbSet[i] = holder
	}
	return mdb
}
// 实现 EmbedDB 接口里的所有方法
func (mdb *MultiDB) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	// 先从客户端连接从获取数据库编号
	db, errReply := mdb.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}

	return db.execWithLock(cmdLine)
}

func (mdb *MultiDB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	selectedDB, errReply := mdb.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectedDB.ExecMulti(conn, watching, cmdLines)
}

func (mdb *MultiDB) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return mdb.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

func (mdb *MultiDB) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	mdb.mustSelectDB(dbIndex).ForEach(cb)
}

func (mdb *MultiDB) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	mdb.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

func (mdb *MultiDB) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	mdb.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

func (mdb *MultiDB) GetDBSize(dbIndex int) (int, int) {
	db := mdb.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
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
	if cmdName == "bgrewriteaof" {
		return BGRewriteAOF(mdb, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		return RewriteAOF(mdb, cmdLine[1:])
	}

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

func (mdb *MultiDB) mustSelectDB(dbIndex int) *DB {
	selectDB, errReply := mdb.selectDB(dbIndex)
	if errReply != nil {
		panic(errReply)
	}
	return selectDB
}


// AfterClientClose does some clean after client close connection
func (mdb *MultiDB) AfterClientClose(c redis.Connection) {

}

// Close graceful shutdown database
func (mdb *MultiDB) Close() {
	// stop replication first

}


func BGRewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	go db.aofHandler.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// RewriteAOF start Append-Only-File rewriting and blocked until it finished
func RewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	err := db.aofHandler.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

