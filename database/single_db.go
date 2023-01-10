package database

import (
	"github.com/hodis/datastruct/dict"
	"github.com/hodis/datastruct/lock"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
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
	cmdSync func(line CmdLine) error
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

// makeBasicDB create DB instance only with basic abilities.
// It is not concurrent safe
func makeBasicDB() *DB {
	db := &DB{
		data: dict.MakeSimple(),
		ttlMap: dict.MakeSimple(),
		versionMap: dict.MakeSimple(),
		locker: lock.Make(1),
		addAof: func(line CmdLine) {},
	}
	return db
}

// Exec executes command within one database
// 实现 github.com/hodis/interface/database/db 里的 DB 接口中的方法
/*
multi
set k v
mset apple 1 google 2 microsoft 3
mget k
mget apple google microsoft
 */
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	//  todo 以后实现 无法在事务中执行的特殊命令
	// 实现 multi 事务（单机版）
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		ret := StartMulti(c)
		logger.Info("Exec: ", string(ret.ToBytes()))
		return ret
	} else if cmdName == "exec" {
		// 执行事务
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "discard" {
		// 丢弃事务
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "watch" {
		/*
			watch 命令，它可以用来监视任意数量的 key，
			如果在 exec 命令执行时，被监视的 key 至少有一个被修改，
			那么事务将拒绝被执行
		*/
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	}
	// 执行事务
	if c != nil && c.InMultiState() {
		//命令入队
		return EnqueueCmd(c, cmdLine)
	}
	// 实现普通命令
	return db.execNormalCommand(cmdLine)
}

// execWithLock executes normal commands, invoker should provide locks
func (db *DB) execWithLock(cmdline [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdline[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	if !validateArity(cmd.arity, cmdline) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	fun := cmd.executor
	return fun(db, cmdline[1:])
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

func (db *DB) Removes(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
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

func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}
		return cb(key, entity, expiration)
	})
}


