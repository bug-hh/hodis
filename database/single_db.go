package database

import (
	"github.com/hodis/datastruct/dict"
	"github.com/hodis/datastruct/lock"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/timewheel"
	"github.com/hodis/redis/protocol"
	"strings"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
)

type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
/*
这个函数是用在开启事务后，用来返回一个事务中所有的写和读命令
 */
type PreFunc func(args [][]byte) ([]string, []string)

type UndoFunc func(db *DB, args [][]byte) []CmdLine

type CmdLine = [][]byte

type DB struct {
	index int

	data dict.Dict
	ttlMap dict.Dict
	versionMap dict.Dict

	locker *lock.Locks
	addAof func(line CmdLine)
}


func MakeDB() *DB {
	return &DB{
		data: dict.MakeConcurrent(dataDictSize),
		ttlMap: dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		locker: lock.Make(dataDictSize),
		addAof: func(line CmdLine) {
		},
	}
}

// Exec executes command within one database
// 实现 github.com/hodis/interface/database/db 里的 DB 接口中的方法
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	//  todo 以后实现 无法在事务中执行的特殊命令

	// 实现普通命令
	return db.execNormalCommand(cmdLine)
}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	prepare := cmd.prepare
	/*
	获取一个事务内所有的写命令和读命令的 key
	 */
	write, read := prepare(cmdLine[1:])
	// 这个 addVersion 有什么用？
	db.addVersion(write...)
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := db.GetVersion(key)
		db.versionMap.Put(key, versionCode+1)
	}
}

func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >=0 {
		return arity == argNum
	}
	return argNum >= -arity
}

/* ---- Lock Function ----- */

// RWLocks lock keys for writing and reading
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

/* ---- TTL Functions ---- */

func genExpireTask(key string) string {
	return "expire:" + key
}

// 给 key 设置过期时间
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)

	// 建立轮询任务
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)

		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}

		et, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(et)
		if expired {
			db.Remove(key)
		}
	})

}

func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}


// Data access
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}

	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true

}

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}



