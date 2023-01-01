package database

import (
	setPackage "github.com/hodis/datastruct/set"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/protocol"
	"strconv"
)

func (db *DB) getAsSet(key string) (setPackage.Set, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	hashSet, ok := entity.Data.(setPackage.Set)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return hashSet, nil
}

func (db *DB) getOrInitSet(key string) (set setPackage.Set, inited bool, errReply protocol.ErrorReply) {
	set, errReply = db.getAsSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if set == nil {
		set = setPackage.NewHashSet()
		db.PutEntity(key, &database.DataEntity{
			Data: set,
		})
		inited = true
	}
	return set, inited, nil
}

//sadd key member [member...]
func execSAdd(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, _, errReply := db.getOrInitSet(key)
	if errReply != nil {
		return errReply
	}

	members := args[1:]
	res := 0
	for _, member := range members {
		res += set.Add(string(member))
	}
	cmdLines := utils.ToCmdLine3("SADD", args...)
	db.addAof(cmdLines)
	syncErr := db.cmdSync(cmdLines)
	if syncErr != nil {
		logger.Warn("sync command `SADD` to slave failed: ", syncErr)
	}
	return protocol.MakeIntReply(int64(res))
}

func undoSAdd(db *DB, args [][]byte) []CmdLine {
	return []CmdLine{
		utils.ToCmdLine3("SREM", args...),
	}
}

func execSCard(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	return protocol.MakeIntReply(int64(set.Len()))
}

func execSISMember(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	member := string(args[1])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil || !set.Contains(member) {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(1)
}

func execSMembers(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil || set.Len() == 0 {
		return protocol.MakeStatusReply("empty array")
	}
	var ret [][]byte
	set.ForEach(func(val string) bool {
		ret = append(ret, []byte(val))
		return true
	})
	return protocol.MakeMultiBulkReply(ret)
}

//srandmembers key [count]
func execSRandMember(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil || set.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}
	count := 1
	var err error
	if len(args) == 2 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply("illegal count")
		}
	}

	allowRepeat := false
	if count < 0 {
		allowRepeat = true
		count *= -1
	}

	size := set.Len()

	var ret [][]byte
	if allowRepeat {
		memberStrs := set.RandomMembers(count)
		for _, memberStr := range memberStrs {
			ret = append(ret, []byte(memberStr))
		}
	} else {
		if count >= size {
			count = size
		}
		memberStrs := set.RandomDistinctMembers(count)
		for _, memberStr := range memberStrs {
			ret = append(ret, []byte(memberStr))
		}
	}

	return protocol.MakeMultiBulkReply(ret)
}

//spop key count
func execSPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	count := 1
	var err error

	if len(args) == 2 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil || count <= 0 {
			return protocol.MakeErrReply("illegal count: " + string(args[1]))
		}
	}

	var ret [][]byte
	memberStrs := set.RandomMembers(count)
	for _, memberStr := range memberStrs {
		ret = append(ret, []byte(memberStr))
		set.Remove(memberStr)
	}

	return protocol.MakeMultiBulkReply(ret)
}

func undoSPop(db *DB, args [][]byte) []CmdLine {
	// 因为是随机的删除元素，所以只能把集合中所有元素都添加一遍
	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil || set == nil {
		return nil
	}

	var members [][]byte
	members = append(members, args[0])
	set.ForEach(func(val string) bool {
		members = append(members, []byte(val))
		return true
	})

	return []CmdLine{
		utils.ToCmdLine3("SADD", members...),
	}
}

//srem key member [members...]
func execSRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil || set.Len() == 0 {
		return protocol.MakeIntReply(0)
	}

	members := args[1:]
	ret := 0
	for _, member := range members {
		if set.Remove(string(member)) {
			ret++
		}
	}

	return protocol.MakeIntReply(int64(ret))
}

func undoSRem(db *DB, args [][]byte) []CmdLine {
	return []CmdLine{
		utils.ToCmdLine3("SADD", args...),
	}
}

//sdiff key1 [key...]
func execSDiff(db *DB, args [][]byte) redis.Reply {
	var ret [][]byte
	key1 := string(args[0])
	set1, errReply := db.getAsSet(key1)
	if errReply != nil || set1 == nil || set1.Len() == 0 {
		return protocol.MakeMultiBulkReply(ret)
	}

	if len(args) == 1 {
		return execSMembers(db, args)
	}
	var res setPackage.Set = setPackage.NewHashSet()
	otherKeys := args[1:]

	for _, otherKey := range otherKeys {
		set, errRelpy := db.getAsSet(string(otherKey))
		if errRelpy != nil || set == nil || set.Len() == 0 {
			continue
		}
		res = res.Union(set)
	}

	diff := set1.Diff(res)
	diff.ForEach(func(val string) bool {
		ret = append(ret, []byte(val))
		return true
	})
	return protocol.MakeMultiBulkReply(ret)
}

//sunion key1 [key...]
func execSUnion(db *DB, args [][]byte) redis.Reply {
	if len(args) == 1 {
		return execSMembers(db, args)
	}

	key1 := string(args[0])
	otherKeys := args[1:]

	var ret setPackage.Set = setPackage.NewHashSet()
	set1, errReply := db.getAsSet(key1)
	if errReply != nil || set1 == nil || set1.Len() == 0 {
		return protocol.MakeMultiBulkReply(nil)
	}
	ret = ret.Union(set1)

	for _, otherKey := range otherKeys {
		otherSet, otherErrReply := db.getAsSet(string(otherKey))
		if otherErrReply != nil || otherSet == nil || otherSet.Len() == 0 {
			continue
		}
		ret = ret.Union(otherSet)
	}

	res := make([][]byte, 0, ret.Len())
	ret.ForEach(func(val string) bool {
		res = append(res, []byte(val))
		return true
	})

	return protocol.MakeMultiBulkReply(res)
}

func init() {
	RegisterCommand("SAdd", execSAdd, writeFirstKey, undoSAdd, -3, flagWrite)
	RegisterCommand("SCard", execSCard, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("SISMember", execSISMember, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("SMembers", execSMembers, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("SRandMember", execSRandMember, readFirstKey, nil, -2, flagReadOnly)
	RegisterCommand("SPop", execSPop, writeFirstKey, undoSPop, -2, flagWrite)
	RegisterCommand("SRem", execSRem, writeFirstKey, undoSRem, -3, flagWrite)
	RegisterCommand("SDiff", execSDiff, readAllKeys, nil, -2, flagReadOnly)
	RegisterCommand("SUnion", execSUnion, readAllKeys, nil, -2, flagWrite)
}
