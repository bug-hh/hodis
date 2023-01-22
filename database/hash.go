package database

import (
	dictPackage "github.com/hodis/datastruct/dict"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/protocol"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"
)

// todo 给每个写操作添加 cmdSync 调用

func (db *DB) getAsDict(key string) (dictPackage.Dict, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	dict, ok := entity.Data.(dictPackage.Dict)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return dict, nil
}

func (db *DB) getOrInitDict(key string) (dict dictPackage.Dict , inited bool, errReply protocol.ErrorReply) {
	dict, errReply = db.getAsDict(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if dict == nil {
		dict = dictPackage.MakeSimple()
		db.PutEntity(key, &database.DataEntity{
			Data: dict,
		})
		inited = true
	}
	return dict, inited, nil
}

//hset key hash_key hash_value
func execHSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dict.Put(field, value)
	db.addAof(utils.ToCmdLine3("hset", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("hset", args...))
		if syncErr != nil {
			logger.Warn("sync hset to slave failed: ", syncErr)
		}
	}
	return protocol.MakeIntReply(int64(result))
}

func undoHSet(db *DB, args [][]byte) []database.CmdLine {
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

func execHGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])

	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dt == nil {
		return &protocol.NullBulkReply{}
	}
	raw, exists := dt.Get(field)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	value, _ := raw.([]byte)
	return protocol.MakeBulkReply(value)
}

func execHExists(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dt == nil {
		return protocol.MakeIntReply(0)
	}
	_, exists := dt.Get(field)
	if exists {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execHSetNX sets field in hash table only if field not exists
func execHSetNX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	dt, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dt.PutIfAbsent(field, value)
	if result > 0 {
		db.addAof(utils.ToCmdLine3("hsetnx", args...))
		// 如果是主从模式，master 将 set 命令发送给 slave
		// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
		if db.cmdSync != nil {
			syncErr := db.cmdSync(utils.ToCmdLine3("hsetnx", args...))
			if syncErr != nil {
				logger.Warn("sync hsetnx to slave failed: ", syncErr)
			}
		}
	}
	return protocol.MakeIntReply(int64(result))
}

func execHDel(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dt == nil {
		return protocol.MakeIntReply(0)
	}
	deleted := 0
	for _, field := range fields {
		result := dt.Remove(field)
		deleted += result
	}

	if dt.Len() == 0 {
		db.Remove(key)
	}

	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("hdel", args...))
		// 如果是主从模式，master 将 set 命令发送给 slave
		// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
		if db.cmdSync != nil {
			syncErr := db.cmdSync(utils.ToCmdLine3("hdel", args...))
			if syncErr != nil {
				logger.Warn("sync hdel to slave failed: ", syncErr)
			}
		}
	}
	return protocol.MakeIntReply(int64(deleted))
}

func undoHDel(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldsArgs := args[1:]
	for i, v := range fieldsArgs {
		fields[i] = string(v)
	}
	return rollbackHashFields(db, key, fields...)
}

func execHLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dt == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(dt.Len()))
}

func execHStrlen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])

	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dt == nil {
		return protocol.MakeIntReply(0)
	}

	raw, exists := dt.Get(field)
	if exists {
		value, _ := raw.([]byte)
		return protocol.MakeIntReply(int64(len(value)))
	}
	return protocol.MakeIntReply(0)
}

func execHMSet(db *DB, args [][]byte) redis.Reply {
	if len(args) % 2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([][]byte, size)

	for i:=0;i<size;i++ {
		fields[i] = string(args[2*i+1])
		values[i] = args[2*i+2]
	}
	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	for i, field := range fields {
		value := values[i]
		dt.Put(field, value)
	}
	db.addAof(utils.ToCmdLine3("hmset", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("hmset", args...))
		if syncErr != nil {
			logger.Warn("sync hmset to slave failed: ", syncErr)
		}
	}
	return &protocol.OkReply{}
}


func undoHMSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args)-1) / 2
	fields := make([]string, size)
	for i:=0;i<size;i++ {
		fields[i] = string(args[2*i+1])
	}
	return rollbackHashFields(db, key, fields...)
}

func execHMGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	size := len(args) - 1
	fields := make([]string, size)
	for i:=0;i<size;i++ {
		fields[i] = string(args[i+1])
	}
	result := make([][]byte, size)
	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}

	if dt == nil {
		return protocol.MakeMultiBulkReply(result)
	}

	for i, field := range fields {
		value, ok := dt.Get(field)
		if !ok {
			result[i] = nil
		} else {
			bytes, _ := value.([]byte)
			result[i] = bytes
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

func execHKeys(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}

	if dt == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	fields := make([][]byte, dt.Len())
	i := 0
	dt.ForEach(func(key string, val interface{}) bool {
		fields[i] = []byte(key)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(fields)
}

func execHVals(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dt == nil {
		return &protocol.EmptyMultiBulkReply{}
	}
	values := make([][]byte, dt.Len())
	i := 0
	dt.ForEach(func(key string, val interface{}) bool {
		values[i], _ = val.([]byte)
		i++
		return true
	})

	return protocol.MakeMultiBulkReply(values)
}

func execHGetAll(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dt == nil {
		return &protocol.EmptyMultiBulkReply{}
	}
	size := dt.Len()
	result := make([][]byte, size*2)
	i := 0
	dt.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		result[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}

//HINCRBY KEY_NAME FIELD_NAME INCR_BY_NUMBER
func execHIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	// 如果哈希表的 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。
	dt, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}
	value, exists := dt.Get(field)
	if !exists {
		dt.Put(field, args[2])
		db.addAof(utils.ToCmdLine3("hincrby", args...))
		// 如果是主从模式，master 将 set 命令发送给 slave
		// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
		if db.cmdSync != nil {
			syncErr := db.cmdSync(utils.ToCmdLine3("hincrby", args...))
			if syncErr != nil {
				logger.Warn("sync hincrby to slave failed: ", syncErr)
			}
		}
		return protocol.MakeBulkReply(args[2])
	}

	val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR hash value is not an integer")
	}
	val += delta
	bytes := []byte(strconv.FormatInt(val, 10))
	dt.Put(field, bytes)
	db.addAof(utils.ToCmdLine3("hincrby", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("hincrby", args...))
		if syncErr != nil {
			logger.Warn("sync hincrby to slave failed: ", syncErr)
		}
	}
	return protocol.MakeBulkReply(bytes)
}

func undoHIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

func execHIncrByFloat(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := decimal.NewFromString(rawDelta)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	dt, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}
	value, exists := dt.Get(field)
	if !exists {
		dt.Put(field, args[2])
		return protocol.MakeBulkReply(args[2])
	}

	val, err := decimal.NewFromString(string(value.([]byte)))
	if err != nil {
		return protocol.MakeErrReply("ERR hash value is not a float")
	}

	result := val.Add(delta)
	resultBytes := []byte(result.String())
	dt.Put(field, resultBytes)
	db.addAof(utils.ToCmdLine3("hincrbyfloat", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("hincrbyfloat", args...))
		if syncErr != nil {
			logger.Warn("sync hincrbyfloat to slave failed: ", syncErr)
		}
	}
	return protocol.MakeBulkReply(resultBytes)
}

/*
从 redis 6.2.0 版本才有的命令
随机的返回 hash 表里的 键
如果 key 不存在，返回 nil
https://redis.io/commands/hrandfield/
hrandfield key [count [withvalues]]
 */
func execHRandField(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count := 1
	withValues := 0
	if len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hrandfield' command")
	}

	if len(args) == 3 {
		if strings.ToLower(string(args[2])) == "withvalues" {
			withValues = 1
		} else {
			return protocol.MakeSyntaxErrReply()
		}
	}

	if len(args) >= 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = int(count64)
	}

	dt, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dt == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	if count > 0 {
		fields := dt.RandomDistinctKeys(count)
		NumField := len(fields)
		if withValues == 0 {
			result := make([][]byte, NumField)
			for i, v := range fields {
				result[i] = []byte(v)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, 2 * NumField)
			for i, v := range fields {
				result[2*i] = []byte(v)
				raw, _ := dt.Get(v)
				result[2*i+1] = raw.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	} else if count < 0 {
		fields := dt.RandomKeys(-count)
		NumField := len(fields)
		if withValues == 0 {
			result := make([][]byte, NumField)
			for i, v := range fields {
				result[i] = []byte(v)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, 2*NumField)
			for i, v := range fields {
				result[2*i] = []byte(v)
				raw, _ := dt.Get(v)
				result[2*i+1] = raw.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	}
	return &protocol.EmptyMultiBulkReply{}
}

func init() {
	RegisterCommand("HSet", execHSet, writeFirstKey, undoHSet, 4, flagWrite)
	RegisterCommand("HSetNX", execHSetNX, writeFirstKey, undoHSet, 4, flagWrite)
	RegisterCommand("HGet", execHGet, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("HExists", execHExists, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("HDel", execHDel, writeFirstKey, undoHDel, -3, flagWrite)
	RegisterCommand("HLen", execHLen, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("HStrlen", execHStrlen, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("HMSet", execHMSet, writeFirstKey, undoHMSet, -4, flagWrite)
	RegisterCommand("HMGet", execHMGet, readFirstKey, nil, -3, flagReadOnly)
	RegisterCommand("HGet", execHGet, readFirstKey, nil, -3, flagReadOnly)
	RegisterCommand("HKeys", execHKeys, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("HVals", execHVals, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("HGetAll", execHGetAll, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("HIncrBy", execHIncrBy, writeFirstKey, undoHIncr, 4, flagWrite)
	RegisterCommand("HIncrByFloat", execHIncrByFloat, writeFirstKey, undoHIncr, 4, flagWrite)
	RegisterCommand("HRandField", execHRandField, readFirstKey, nil, -2, flagReadOnly)
}
