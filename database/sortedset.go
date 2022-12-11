package database

import (
	"github.com/hodis/datastruct/sortedset"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/protocol"
	"strconv"
	"strings"
)

func (db *DB) getAsSortedSet(key string) (*sortedset.SortedSet, protocol.ErrorReply) {
	// 先从 dict 中取出数据
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	sortedSet, ok := entity.Data.(*sortedset.SortedSet)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return sortedSet, nil
}

func (db *DB) getOrInitSortedSet(key string) (sortedSet *sortedset.SortedSet, inited bool, errReply protocol.ErrorReply) {
	// 先去 get，没有的话，就新建一个
	sortedSet, errReply = db.getAsSortedSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if sortedSet == nil {
		sortedSet = sortedset.Make()
		db.PutEntity(key, &database.DataEntity{
			Data: sortedSet,
		})
		inited = true
	}
	return sortedSet, inited, nil
}

func execZAdd(db *DB, args [][]byte) redis.Reply {
	/*
	ZADD KEY_NAME SCORE1 VALUE1.. SCOREN VALUEN
	除去 ZADD，剩下的参数部分一定是一个奇数
	 */
	if len(args) %2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}
	key := string(args[0])
	// 这是计算有多少个 value
	size := (len(args) - 1) / 2
	elements := make([]*sortedset.Element, size)
	for i:=0;i<size;i++ {
		scoreValue := args[2*i+1]
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(scoreValue), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}

		elements[i] = &sortedset.Element{
			Member: member,
			Score: score,
		}
	}

	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}

	i := 0
	for _, e := range elements {
		if sortedSet.Add(e.Member, e.Score) {
			i++
		}
	}
	logger.Debug("add member: ", sortedSet.GetSkipList().GetHeader().GetLevel()[0].GetForward().Member)
	logger.Debug("add score: ", sortedSet.GetSkipList().GetHeader().GetLevel()[0].GetForward().Score)

	db.addAof(utils.ToCmdLine3("zadd", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("zadd", args...))
		if syncErr != nil {
			logger.Warn("sync zadd to slave failed: ", syncErr)
		}
	}

	return protocol.MakeIntReply(int64(i))
}

//ZRANGE key start stop [WITHSCORES]
func execZRange(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrange' command")
	}
	// 为 true 的话，表示把对应分数也展示出来
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return protocol.MakeErrReply("syntax error")
		}
		withScores = true
	}

	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, false)
}

func execZRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		logger.Info("getAsSortedSet ERR: ", errReply.Error())
		return errReply
	}

	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	var deleted int64 = 0
	for _, field := range fields {
		if sortedSet.Remove(field) {
			deleted++
		}
	}

	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("zrem", args...))
		// 如果是主从模式，master 将 set 命令发送给 slave
		// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
		if db.cmdSync != nil {
			syncErr := db.cmdSync(utils.ToCmdLine3("zrem", args...))
			if syncErr != nil {
				logger.Warn("sync zrem to slave failed: ", syncErr)
			}
		}
	}
	return protocol.MakeIntReply(deleted)
}

func range0(db *DB, key string, start int64, stop int64, withScores bool, desc bool) redis.Reply {
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}

	if sortedSet == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	size := sortedSet.Len()
	if start < -1 * size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &protocol.EmptyMultiBulkReply{}
	}
	if stop < -1 * size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop+1
	} else if stop < size {
		stop++
	} else {
		stop = size

	}
	if stop < start {
		stop = start
	}

	slice := sortedSet.Range(start, stop, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr)
			i++
		}
		return protocol.MakeMultiBulkReply(result)
	}

	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result)
}

func undoZAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args)-1) / 2
	fields := make([]string, size)
	for i:=0;i<size;i++ {
		fields[i] = string(args[2*i+2])
	}
	return rollbackZSetFields(db, key, fields...)
}

func undoZRem(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	return rollbackZSetFields(db, key, fields...)
}

func init() {
	RegisterCommand("ZAdd", execZAdd, writeFirstKey, undoZAdd, -4, flagWrite)
	RegisterCommand("ZRange", execZRange, readFirstKey, nil, -4, flagReadOnly)
	RegisterCommand("ZRem", execZRem, writeFirstKey, undoZRem, -3, flagWrite)
}