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

//rpop key [count]
func execRPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	retList, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if retList == nil {
		return &protocol.NullBulkReply{}
	}
	count := 1
	var err error
	if len(args) == 2 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply("illegal count")
		}
	}
	var result [][]byte
	for i:=0;i<count;i++ {
		val, _ := retList.RemoveLast().([]byte)
		result = append(result, val)
	}

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
	return protocol.MakeMultiBulkReply(result)
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

//LPOP key [count]
// args 只包含 key [count]
func undoLPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	retList, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if retList == nil || retList.Len() == 0 {
		return nil
	}
	count := 1
	if len(args) == 2 {
		//出错的话，count 默认按 1 算
		count, _ = strconv.Atoi(string(args[1]))
	}
	if (count < 0 && retList.Len() + count < 0) || count > retList.Len() {
		return nil
	}
	if count < 0 {
		count = retList.Len() + count
	}
	rawElements := retList.Range(0, count)
	cmdLines := make([]CmdLine, 0, count)

	for _, element := range rawElements {
		bs := element.([]byte)
		cmdLines = append(cmdLines, utils.ToCmdLine3("LPUSH", bs))
	}
	return cmdLines
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

//lpop key [count]
func execLPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if len(args) > 2 {
		return protocol.MakeArgNumErrReply("lpop")
	}
	var err error
	count := 1
	if len(args) == 2 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply("illegal count")
		}
	}

	ls, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if ls == nil {
		logger.Info("list is nil")
		return &protocol.NullBulkReply{}
	}

	var result [][]byte
	for i:=0;i<count && ls.Len() > 0;i++ {
		deletedVal := ls.Remove(0)
		val, _ := deletedVal.([]byte)
		result = append(result, val)
	}
	db.addAof(utils.ToCmdLine3("lpop", args...))
	// 如果是主从模式，master 将 set 命令发送给 slave
	// 这是一个从外部传入的回调函数, 只有 master 节点才能执行，只有 master 节点会初始化 cmdSync 字段
	if db.cmdSync != nil {
		syncErr := db.cmdSync(utils.ToCmdLine3("lpop", args...))
		if syncErr != nil {
			logger.Warn("sync rpop to slave failed: ", syncErr)
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

//LINDEX key index
// 这里 index 可以是负数，表示从结尾开始数，-1 表示倒数第一个，-2 表示倒数第二个
func execLIndex(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal index")
	}

	ls, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ls == nil {
		return protocol.MakeNullBulkReply()
	}
	size := ls.Len()
	if (index < 0 && size + index < 0) || index >= size {
		return protocol.MakeErrReply("index out of range")
	}

	if index < 0 {
		index = size + index
	}

	raw := ls.Get(index)
	val, _ := raw.([]byte)
	return protocol.MakeBulkReply(val)
}

//llen key
func execLLen(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("LLEN")
	}
	key := string(args[0])
	ls, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ls == nil {
		return protocol.MakeErrReply("the list does not exists")
	}

	return protocol.MakeIntReply(int64(ls.Len()))
}
//linsert key <before | after> pivot element
func execLInsert(db *DB, args [][]byte) redis.Reply {
	if len(args) != 4 {
		return protocol.MakeArgNumErrReply("LINSERT")
	}

	key := string(args[0])
	ls, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if ls == nil {
		return protocol.MakeNullBulkReply()
	}

	pivot := args[2]
	index := ls.IndexOfVal(func(a interface{}) bool {
		bs := a.([]byte)
		return utils.BytesEquals(pivot, bs)
	})

	// pivot 不存在
	if index == -1 {
		return protocol.MakeIntReply(-1)
	}
	element := args[3]
	if string(args[1]) == "after" {
		ls.Insert(index+1, element)
	} else {
		ls.Insert(index, element)
	}
	return protocol.MakeIntReply(int64(ls.Len()))
}

//LInsert key <before | after> pivot element
func undoLInsert(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	ls, errReply := db.getAsList(key)
	if errReply != nil || ls == nil || ls.Len() == 0 {
		return nil
	}

	element := args[3]
	index := ls.IndexOfVal(func(a interface{}) bool {
		bs := a.([]byte)
		return utils.BytesEquals(bs, element)
	})
	if index == -1 {
		return nil
	}

	return []CmdLine{
		// LRem key count element
		utils.ToCmdLine2("LREM", key, "1", string(element)),
	}
}

// lrem key count element
func execLRem(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("LREM")
	}
	key := string(args[0])
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal count")
	}
	ls, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if ls == nil || ls.Len() == 0 {
		return protocol.MakeIntReply(0)
	}
	element := args[2]
	ret := ls.RemoveByVal(func(a interface{}) bool {
		bs := a.([]byte)
		return utils.BytesEquals(bs, element)
	}, count)
	return protocol.MakeIntReply(int64(ret))
}

// lrem key count element
func undoLRem(db *DB, args [][]byte) []CmdLine {
	if len(args) != 3 {
		return nil
	}
	key := string(args[0])
	ls, errReply := db.getAsList(key)
	if errReply != nil || ls == nil {
		return nil
	}

	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil
	}

	element := args[2]

	ret := make([]CmdLine, 0, count)
	for i:=0;i<count;i++ {
		ret = append(ret, utils.ToCmdLine3("RPUSH", element))
	}
	return ret
}

//ltrim key start stop 包含 stop
func execLTrim(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ls, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if ls == nil {
		return protocol.MakeNullBulkReply()
	}

	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal start")
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply("illegal stop")
	}

	size := ls.Len()
	if start < 0 {
		start = size + start
	}

	if stop < 0 {
		stop = size + stop
	}

	if start >= size || start > stop {
		start = 0
		stop = size-1
	}

	// 移除 [start, stop] 以外的元素
	ls.Trim(start, stop)
	return protocol.MakeOkReply()
}

//ltrim key start stop 包含 stop
func undoLTrim(db *DB, args [][]byte) []CmdLine {
	if len(args) != 3 {
		return nil
	}
	key := string(args[0])
	ls, errReply := db.getAsList(key)
	if errReply != nil || ls == nil {
		return nil
	}

	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil
	}

	size := ls.Len()
	if start < 0 {
		start = size + start
	}

	if stop < 0 {
		stop = size + stop
	}

	if start >= size || start > stop {
		start = 0
		stop = size-1
	}

	var ret []CmdLine
	for i:=start-1;i>=0;i-- {
		bs := ls.Get(i).([]byte)
		ret = append(ret, utils.ToCmdLine3("LPUSH", args[0], bs))
	}

	for i:=stop+1;i<size;i++ {
		bs := ls.Get(i).([]byte)
		ret = append(ret, utils.ToCmdLine3("RPUSH", args[0], bs))
	}
	return ret
}

//LSET key index element
func execLSet(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("LSET")
	}
	key := string(args[0])
	ls, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if ls == nil {
		return protocol.MakeNullBulkReply()
	}

	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal index")
	}

	if index < 0 {
		index = ls.Len() + index
	}
	if index < 0 || index >= ls.Len() {
		return protocol.MakeErrReply("index out of range")
	}
	element := args[2]
	ls.Set(index, element)
	return protocol.MakeOkReply()
}

func undoLSet(db *DB, args [][]byte) []CmdLine {
	if len(args) != 3 {
		return nil
	}
	key := string(args[0])
	ls, errReply := db.getAsList(key)
	if errReply != nil || ls == nil {
		return nil
	}

	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil
	}

	if index < 0 {
		index = ls.Len() + index
	}

	if index < 0 || index >= ls.Len() {
		return nil
	}
	// 把原来的值取出来
	oldValue := ls.Get(index)
	bs := oldValue.([]byte)
	return []CmdLine{
		utils.ToCmdLine3("LSET", args[0], args[1], bs),
	}
}

func init() {
	RegisterCommand("LPush", execLPush, writeFirstKey, undoLPush, -3, flagWrite)
	RegisterCommand("RPush", execRPush, writeFirstKey, undoRPush, -3, flagWrite)

	RegisterCommand("LRange", execLRange, readFirstKey, nil, 4, flagReadOnly)

	RegisterCommand("LPop", execLPop, writeFirstKey, undoLPop, -2, flagWrite)
	RegisterCommand("RPop", execRPop, writeFirstKey, undoRPop, -2, flagWrite)

	RegisterCommand("LIndex", execLIndex, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("LLen", execLLen, readFirstKey, nil, 2, flagReadOnly)

	RegisterCommand("LInsert", execLInsert, writeFirstKey, undoLInsert, 5, flagReadOnly)
	RegisterCommand("LRem", execLRem, writeFirstKey, undoLRem, 4, flagWrite)
	RegisterCommand("LTrim", execLTrim, writeFirstKey, undoLTrim, 4, flagWrite)

	RegisterCommand("LSet", execLSet, writeFirstKey, undoLSet, 4, flagWrite)
}
