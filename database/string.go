package database

import (
	"github.com/hodis/aof"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
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
					protocol.MakeErrReply("ERR invalid expire time in set")
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
					protocol.MakeErrReply("ERR invalid expire time in set")
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
	if result > 0 {
		return &protocol.OkReply{}
	}
	return &protocol.NullBulkReply{}
}




func init() {
	RegisterCommand("Get", execGet, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3, flagWrite)




}