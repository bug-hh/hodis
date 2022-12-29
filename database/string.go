package database

import (
	"github.com/hodis/aof"
	"github.com/hodis/datastruct/bits"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/protocol"
	"strconv"
	"strings"
	"time"
)

const (
	upsertPolicy = iota // default
	insertPolicy        // set nx
	updatePolicy        // set ex
)

const unlimitedTTL int64 = 0

func (db *DB) getAsBitMap(key string) (bits.BinaryBit, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.(bits.BinaryBit)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return bytes, nil
}

func (db *DB) getAsString(key string) ([]byte, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return bytes, nil
}

func execGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply(bytes)
}

/*
SET key value [EX seconds|PX milliseconds|KEEPTTL] [NX|XX] [GET]
	从2.6.12版本开始，redis为SET命令增加了一系列选项:

	EX seconds – 设置键key的过期时间，单位时秒
	PX milliseconds – 设置键key的过期时间，单位时毫秒
	NX – 只有键key不存在的时候才会设置key的值
	XX – 只有键key存在的时候才会设置key的值
	KEEPTTL -- 获取 key 的过期时间
	GET -- 返回 key 存储的值，如果 key 不存在返回空
 */
func execSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]
	policy := upsertPolicy
	ttl := unlimitedTTL

	if len(args) > 2 {
		for i:=2;i<len(args);i++ {
			arg := strings.ToUpper(string(args[i]))
			if arg == "NX" {
				if policy == updatePolicy {
					return &protocol.SyntaxErrReply{}
				}
				policy = insertPolicy
			} else if arg == "XX" {
				if policy == insertPolicy {
					return &protocol.SyntaxErrReply{}
				}
				policy = updatePolicy
			} else if arg == "EX" {
				if ttl != unlimitedTTL {
					return &protocol.SyntaxErrReply{}
				}
				if i + 1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}

				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}

				if ttlArg <= 0 {
					return protocol.MakeErrReply("ERR invalid expire time in set")
				}
				// 转换成毫秒
				ttl = ttlArg * 1000
				i++
			} else if arg == "PX" {
				if ttl != unlimitedTTL {
					return &protocol.SyntaxErrReply{}
				}
				if i+1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}
				if ttlArg <= 0 {
					return protocol.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg
				i++
			} else {
				return &protocol.SyntaxErrReply{}
			}
		}
	}

	entity := &database.DataEntity{
		Data: value,
	}

	var result int
	switch policy {
	case upsertPolicy:
		db.PutEntity(key, entity)
		result = 1
	case insertPolicy:
		result = db.PutIfAbsent(key, entity)
	case updatePolicy:
		result = db.PutIfExists(key, entity)
	}

	if result > 0 {
		// 这个 key 是有设置过期时间的
		/*
		这里做 aof 写入时，对于 set key value EX seconds 这种指定了过期时间的命令，用下面两条命令进行改写
		set key value
		pexpireat key 毫秒时间戳
		 */
		if ttl != unlimitedTTL {
			// 根据过期时长，算出过期时间
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			// 给 key 设置过期时间
			db.Expire(key, expireTime)
			db.addAof(CmdLine{
				[]byte("SET"),
				args[0],
				args[1],
			})
			db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
		} else {
			db.Persist(key)
			db.addAof(utils.ToCmdLine3("set", args...))
		}
	}
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		logger.Info("set sync")
		syncErr := db.cmdSync(utils.ToCmdLine3("set", args...))
		if syncErr != nil {
			logger.Warn("sync command to slave failed: ", syncErr)
		}
	}

	if result > 0 {
		return &protocol.OkReply{}
	}
	return &protocol.NullBulkReply{}
}

func prepareMGet(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}

func execMGet(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	result := make([][]byte, len(args))
	for i, key := range keys {
		bytes, err := db.getAsString(key)
		if err != nil {
			_, isWrongType := err.(*protocol.WrongTypeErrReply)
			if isWrongType {
				result[i] = nil
				continue
			} else {
				return err
			}
		}
		result[i] = bytes
	}
	return protocol.MakeMultiBulkReply(result)
}

func prepareMSet(args [][]byte) ([]string, []string) {
	size := len(args) / 2
	keys := make([]string, size)
	for i:=0;i<size;i++ {
		keys[i] = string(args[2*i])
	}
	return keys, nil
}

func undoMSet(db *DB, args [][]byte) []database.CmdLine {
	writeKeys, _ := prepareMSet(args)
	return rollbackGivenKeys(db, writeKeys...)

}
func execMSet(db *DB, args [][]byte) redis.Reply {
	if len(args) % 2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}
	size := len(args) / 2
	keys := make([]string, size)
	values := make([][]byte, size)

	for i:=0;i<size;i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}
	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}
	db.addAof(utils.ToCmdLine3("mset", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("mset", args...))
		if syncErr != nil {
			logger.Warn("sync mset to slave failed: ", syncErr)
		}
	}
	return &protocol.OkReply{}
}

//setbits key offset value
func execSetBit(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("setbits")
	}

	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("convert offset error, " + err.Error())
	}

	value := 0
	if string(args[2]) == "1" {
		value = 1
	}

	bs, err := db.getAsBitMap(key)
	if err != nil {
		return protocol.MakeErrReply("get bits array error, " + err.Error())
	}
	oldValue := 0
	if bs != nil && len(bs) > 0 {
		oldValue = int(bs.GetBits(offset))
	}
	bs.SetBits(offset, value)
	db.PutEntity(key, &database.DataEntity{Data: bs})
	return protocol.MakeIntReply(int64(oldValue))
}

//getbits key offset
func execGetBit(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("getbit")
	}

	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("convert offset error, " + err.Error())
	}

	bs, err := db.getAsBitMap(key)
	if err != nil {
		return protocol.MakeErrReply("get bits array error, " + err.Error())
	}
	if bs == nil || len(bs) == 0 {
		return protocol.MakeNullBulkReply()
	}

	return protocol.MakeIntReply(int64(bs.GetBits(offset)))
}

//bitcount key [start] [end]
func execBitCount(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	start := 0
	end := -1
	var err error
	var bs bits.BinaryBit
	if len(args) == 2 {
		return protocol.MakeErrReply("syntax error")
	}
	if len(args) == 3 {
		start, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply("illegal start")
		}

		end, err = strconv.Atoi(string(args[2]))
		if err != nil {
			return protocol.MakeErrReply("illegal end")
		}
	}

	bs, err = db.getAsBitMap(key)
	if err != nil {
		return protocol.MakeErrReply("get bits array error, " + err.Error())
	}

	return protocol.MakeIntReply(int64(bs.BitsCount(start, end)))
}

func init() {
	RegisterCommand("Get", execGet, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3, flagWrite)

	RegisterCommand("MGet", execMGet, prepareMGet, nil, -2, flagReadOnly)
	RegisterCommand("MSet", execMSet, prepareMSet, undoMSet, -3, flagWrite)

	RegisterCommand("SetBit", execSetBit, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("GetBit", execGetBit, writeFirstKey, nil, 3, flagReadOnly)

	RegisterCommand("BitCount", execBitCount, readFirstKey, nil, -2, flagReadOnly)
}