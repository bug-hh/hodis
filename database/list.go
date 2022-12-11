package database

import (
	"github.com/hodis/datastruct/list"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/protocol"
	"strconv"
)

func (db *DB) getAsList(key string) (list.List, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	myList, ok := entity.Data.(list.List)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return myList, nil
}

func (db DB) getOrInitList(key string) (retList list.List, isNew bool, errReply protocol.ErrorReply) {
	retList, errReply = db.getAsList(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isNew = false
	if retList == nil {
		retList = list.NewQuickList()
		db.PutEntity(key, &database.DataEntity{
			Data: retList,
		})
		isNew = true
	}
	return retList, isNew, nil
}

func execLPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]
	retList, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}
	for _, value := range values {
		retList.Insert(0, value)
	}
	db.addAof(utils.ToCmdLine3("lpush", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("lpush", args...))
		if syncErr != nil {
			logger.Warn("sync lpush to slave failed: ", syncErr)
		}
	}
	return protocol.MakeIntReply(int64(retList.Len()))
}

func undoLPush(db *DB, args [][]byte) []database.CmdLine {
	key := string(args[0])
	count := len(args)-1
	cmdLines := make([]database.CmdLine, 0, count)
	for i:=0;i<count;i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("LPOP", key))
	}
	return cmdLines
}


func execLRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR start value is not an integer or out of range")
	}
	start := int(start64)

	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR stop value is not an integer or out of range")
	}
	stop := int(stop64)

	retList, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if retList == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	size := retList.Len()
	if start < -1 * size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &protocol.EmptyMultiBulkReply{}
	}

	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}

	if stop < start {
		stop = start
	}

	slice := retList.Range(start, stop)
	result := make([][]byte, len(slice))
	for i, raw := range slice {
		bytes, _ := raw.([]byte)
		result[i] = bytes
	}

	return protocol.MakeMultiBulkReply(result)
}

func execRPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	retList, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if retList == nil {
		return &protocol.NullBulkReply{}
	}

	val, _ := retList.RemoveLast().([]byte)
	if retList.Len() == 0 {
		db.Remove(key)
	}
	db.addAof(utils.ToCmdLine3("rpop", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("rpop", args...))
		if syncErr != nil {
			logger.Warn("sync rpop to slave failed: ", syncErr)
		}
	}
	return protocol.MakeBulkReply(val)
}

var rPushCmd = []byte("RPUSH")

func undoRPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	retList, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if retList == nil || retList.Len() == 0 {
		return nil
	}

	element, _ := retList.Get(retList.Len()-1).([]byte)
	return []database.CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},

	}

}

func execRPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	retList, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	for _, value := range values {
		retList.Add(value)
	}

	db.addAof(utils.ToCmdLine3("rpush", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("rpush", args...))
		if syncErr != nil {
			logger.Warn("sync rpush to slave failed: ", syncErr)
		}
	}
	return protocol.MakeIntReply(int64(retList.Len()))
}

func undoRPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]database.CmdLine, 0, count)

	for i:=0;i<count;i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("RPOP", key))
	}
	return cmdLines
}


func init() {
	RegisterCommand("LPush", execLPush, writeFirstKey, undoLPush, -3, flagWrite)
	RegisterCommand("RPush", execRPush, writeFirstKey, undoRPush, -3, flagWrite)

	RegisterCommand("LRange", execLRange, readFirstKey, nil, 4, flagReadOnly)
	RegisterCommand("RPop", execRPop, writeFirstKey, undoRPop, 2, flagWrite)
}
