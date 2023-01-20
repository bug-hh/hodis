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

func judgeOptions(isNX, isGT, isLT bool) bool {
	return (!isNX && !isGT && !isLT) ||
		   (isGT && !isLT && !isNX) ||
		   (isLT && !isGT && !isNX) ||
		   (isNX && !isGT && !isLT)
}

func judgeExistOption(isNX, isXX bool) bool {
	return !(isNX && isXX)
}

func execZAdd(db *DB, args [][]byte) redis.Reply {
	/*
	ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member...]
	 */
	key := string(args[0])
	otherArgs := args[1:]
	var isNX, isXX, isGT, isLT, isCH, isINCR bool
	scoreStartIndex := -1
	for i, arg := range otherArgs {
		argStr := string(arg)
		if !isNX {
			isNX = strings.ToUpper(argStr) == "NX"
		}
		if !isXX {
			isXX = strings.ToUpper(argStr) == "XX"
		}

		if !isGT {
			isGT = strings.ToUpper(argStr) == "GT"
		}

		if !isLT {
			isLT = strings.ToUpper(argStr) == "LT"
		}

		if !isCH {
			isCH = strings.ToUpper(argStr) == "CH"
		}

		if !isINCR {
			isINCR = strings.ToUpper(argStr) == "INCR"
		}
		_, err := strconv.ParseFloat(argStr, 64)
		if err == nil && scoreStartIndex == -1 {
			scoreStartIndex = i
		}
	}

	// NX GT LT 三个是互斥的
	if !judgeOptions(isNX, isGT, isLT) {
		return protocol.MakeErrReply("The GT, LT and NX options are mutually exclusive.")
	}

	// NX XX 是互斥的
	if !judgeExistOption(isNX, isXX) {
		return protocol.MakeErrReply("XX and NX options at the same time are not compatible")
	}

	policy := utils.UpsertPolicy
	if isNX {
		policy = utils.InsertPolicy
	} else if isXX {
		policy = utils.UpdatePolicy
	}

	scoreUpdatePolicy := utils.SORTED_SET_UPDATE_ALL
	if isGT {
		scoreUpdatePolicy = utils.SORTED_SET_UPDATE_GREATER
	} else if isLT {
		scoreUpdatePolicy = utils.SORTED_SET_UPDATE_LESS_THAN
	}
	logger.Info("scoreStartIndex: ", scoreStartIndex)
	if scoreStartIndex == -1 {
		return protocol.MakeSyntaxErrReply()
	}
	// 这是计算有多少个 value
	elementsArgs := otherArgs[scoreStartIndex:]
	size := len(elementsArgs)/2
	if isINCR && size != 1 {
		return protocol.MakeErrReply("INCR option supports a single increment-element pair")
	}
	logger.Info("size: ", size)
	logger.Info("elementsArgs: ", elementsArgs)
	elements := make([]*sortedset.Element, size)
	for i:=0;i<size;i++ {
		scoreValue := elementsArgs[2*i]
		member := string(elementsArgs[2*i+1])
		score, err := strconv.ParseFloat(string(scoreValue), 64)
		logger.Info("member: ", member)
		logger.Info("score: ", score)
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
		if sortedSet.Add(e.Member, e.Score, policy, scoreUpdatePolicy, isCH, isINCR) {
			i++
			logger.Debug("add member: ", e.Member)
			logger.Debug("add score: ", e.Score)
		}
	}


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

//ZRANGE key start stop [withscores]
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

//ZREM key member [member ...]
func execZRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
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
	logger.Info("key: ", key)
	if sortedSet == nil || sortedSet.Len() == 0 {
		return &protocol.EmptyMultiBulkReply{}
	}

	logger.Info("sortedSet len: ", sortedSet.Len())

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

func execZCard(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	zset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}

	if zset == nil {
		return protocol.MakeIntReply(0)
	}

	return protocol.MakeIntReply(zset.Len())
}

//ZCOUNT key min max
func execZCount(db *DB, args [][]byte) redis.Reply {
	var min, max float64
	var err error
	key := string(args[0])
	minStr := strings.ToLower(string(args[1]))
	maxStr := strings.ToLower(string(args[2]))

	isMinInf := minStr == "-inf"
	isMaxInf := maxStr == "+inf"

	exclusiveMin := strings.HasPrefix(minStr, "(") && !isMinInf
	exclusiveMax := strings.HasPrefix(maxStr, "(") && !isMaxInf

	if exclusiveMin {
		minStr = minStr[1:]
	}

	if exclusiveMax {
		maxStr = maxStr[1:]
	}

	if !isMinInf {
		min, err = strconv.ParseFloat(minStr, 64)
		if err != nil {
			return protocol.MakeErrReply("illegal min value")
		}
		if exclusiveMin {
			min++
		}
	}

	if !isMaxInf {
		max, err = strconv.ParseFloat(maxStr, 64)
		if err != nil {
			return protocol.MakeErrReply("illegal max value")
		}
		if exclusiveMax {
			max--
		}
	}

	zset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}

	if zset == nil || zset.Len() == 0 || min > max {
		return protocol.MakeIntReply(0)
	}

	if isMinInf && isMaxInf {
		return protocol.MakeIntReply(zset.Len())
	}

	var start int64 = 0
	stop := zset.Len()

	ret := 0
	zset.ForEach(start, stop, false, func(element *sortedset.Element) bool {
		score := element.Score
		if !isMinInf && !isMaxInf && score >= min && score <= max {
			ret++
		} else if isMinInf && score <= max {
			ret++
		} else if isMaxInf && score >= min {
			ret++
		}
		return true
	})

	return protocol.MakeIntReply(int64(ret))
}

func execZRevRange(db *DB, args [][]byte) redis.Reply {
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
	return range0(db, key, start, stop, withScores, true)
}

//zrank key member
func execZRank(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	zset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}

	if zset == nil || zset.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}
	member := string(args[1])
	return protocol.MakeIntReply(int64(zset.Rank(member, false)))
}

func execZRevRank(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	zset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}

	if zset == nil || zset.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}
	member := string(args[1])
	return protocol.MakeIntReply(int64(zset.Rank(member, true)))
}

//ZSCORE key member
func execZScore(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	zset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}

	if zset == nil || zset.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}
	member := string(args[1])
	element, exists := zset.Get(member)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeStatusReply(strconv.FormatFloat(element.Score, 'f', -1, 64))
}

//zdiffstore dest numkeys key [key...]
func execZDiffStore(db *DB, args [][]byte) redis.Reply {
	destKey := string(args[0])
	destSet, _, _ := db.getOrInitSortedSet(destKey)

	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal numkeys")
	}
	var res [][]byte
	if numKeys == 1 {
		key := string(args[2])
		srcSet, errReply := db.getAsSortedSet(key)
		if errReply != nil {
			return errReply
		}
		if srcSet == nil || srcSet.Len() == 0 {
			return protocol.MakeNullBulkReply()
		}
		size := srcSet.Len()
		destSet = destSet.Union(srcSet)

		srcSet.ForEach(0, size, false, func(element *sortedset.Element) bool {
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			res = append(res, []byte(element.Member), []byte(scoreStr))
			return true
		})
		return protocol.MakeMultiBulkReply(res)
	}

	keys := args[2:]
	firstKeySet, errReply := db.getAsSortedSet(string(keys[0]))
	if errReply != nil {
		return errReply
	}

	for i:=1;i<numKeys;i++ {
		otherKeySet, errReply := db.getAsSortedSet(string(keys[i]))
		if errReply != nil || otherKeySet == nil || otherKeySet.Len() == 0 {
			continue
		}
		destSet = destSet.Union(otherKeySet)
	}

	ret := firstKeySet.Diff(destSet)
	db.PutEntity(destKey, &database.DataEntity{Data: ret})

	return protocol.MakeIntReply(ret.Len())
}

func undoZDiffStore(db *DB, args [][]byte) []CmdLine {
	return []CmdLine {
		utils.ToCmdLine3("Del", args[0]),
	}
}

//zdiff numkeys key [key...] [withsocres]
func execZDiff(db *DB, args [][]byte) redis.Reply {
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("illegal numkeys")
	}
	withScores := strings.ToLower(string(args[len(args)-1])) == "withscores"
	n := len(args[1:])
	if withScores {
		n--
	}
	if n != numKeys {
		return protocol.MakeErrReply("numKeys is not equal to actual number of keys")
	}
	var res [][]byte
	firstKey := string(args[1])
	firstKeySet, errReply := db.getAsSortedSet(firstKey)
	if errReply != nil {
		return errReply
	}
	var start, stop int64
	if numKeys == 1 {
		if firstKeySet == nil || firstKeySet.Len() == 0 {
			return protocol.MakeEmptyMultiBulkReply()
		}

		stop = firstKeySet.Len()
		firstKeySet.ForEach(start, stop, false, func(element *sortedset.Element) bool {
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			res = append(res, []byte(element.Member), []byte(scoreStr))
			return true
		})
		return protocol.MakeMultiBulkReply(res)
	}
	temp := sortedset.Make()
	keyArgs := args[1:]
	for i:=1;i<numKeys;i++ {
		otherKeySet, errReply := db.getAsSortedSet(string(keyArgs[i]))
		if errReply != nil || otherKeySet == nil || otherKeySet.Len() == 0 {
			continue
		}
		temp = temp.Union(otherKeySet)
	}

	diff := firstKeySet.Diff(temp)
	start = 0
	stop = diff.Len()
	diff.ForEach(start, stop, false, func(element *sortedset.Element) bool {
		scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
		res = append(res, []byte(element.Member), []byte(scoreStr))
		return true
	})
	return protocol.MakeMultiBulkReply(res)
}

// ZINCRBY key increment member
func execZIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	increment, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return protocol.MakeErrReply("illegal increment")
	}

	member := string(args[2])
	zset, _, _ := db.getOrInitSortedSet(key)
	var result float64
	element, exists := zset.Get(member)
	isIncr := true
	if !exists {
		isIncr = false
		result = increment
	} else {
		result = increment + element.Score
	}
	logger.Info("member: ", member)
	logger.Info("increment: ", increment)
	logger.Info("isIncr: ", isIncr)
	logger.Info("exists: ", exists)
	zset.Add(member, increment, utils.UpsertPolicy, utils.SORTED_SET_UPDATE_ALL, false, isIncr)
	return protocol.MakeStatusReply(strconv.FormatFloat(result, 'f', -1, 64))
}

// ZINCRBY key increment member
func undoZIncrBy(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	increment, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return nil
	}
	member := string(args[2])
	zset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return nil
	}
	// 回滚分情况，第一种，这个 key 对应的 sortedset 本来就没有
	// 那么这个时候，就直接 del key
	if zset == nil {
		return []CmdLine{
			utils.ToCmdLine3("DEL", args[0]),
		}
	}

	_, exists := zset.Get(member)
	// 第二种 这个 key 对应的 sortedset 有，这种条件下，再分两种情况
	// 1. member 存在  原来是 score += increment，那么回滚就是 score -= increment
	if exists {
		incrStr := strconv.FormatFloat(-increment, 'f', -1, 64)
		return []CmdLine{
			utils.ToCmdLine2("ZADD", "XX", "INCR", incrStr, member),
		}
	}
	// 2. member 不存在  原来会创建 member 那么回滚 remove(member)
	return []CmdLine{
		// zrem key member
		utils.ToCmdLine3("ZREM", args[0], args[2]),
	}
}
// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight
//  [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
func execZUnionStore(db *DB, args [][]byte) redis.Reply {
	destKey := string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal numkeys")
	}

	weights := make([]float64, numKeys)
	aggregate := utils.AGGREGATE_SUM


	weightsIndex := -1
	aggregateIndex := -1

	for i, arg := range args {
		argStr := strings.ToLower(string(arg))
		if argStr == "weights" {
			weightsIndex = i
		} else if argStr == "aggregate" {
			aggregateIndex = i
		}
	}

	if weightsIndex != -1 {
		j := weightsIndex+1
		for i:=0;i<numKeys;i++ {
			w, err := strconv.ParseFloat(string(args[j]), 64)
			if err != nil {
				return protocol.MakeErrReply("illegal weight: " + string(args[j]))
			}
			weights[i] = w
			j++
		}
	} else {
		for i:=0;i<numKeys;i++ {
			weights[i] = 1
		}
	}

	if aggregateIndex != -1 {
		aggrOption := strings.ToLower(string(args[aggregateIndex+1]))
		if aggrOption == "min" {
			aggregate = utils.AGGREGATE_MIN
		} else if aggrOption == "max" {
			aggregate = utils.AGGREGATE_MAX
		}
	}

	sets := make([]*sortedset.SortedSet, numKeys)
	i := 2
	for j:=0;j<numKeys;j++ {
		key := string(args[i])
		sets[j], _ = db.getAsSortedSet(key)
		i++
	}

	ret := sortedset.Unions(sets, weights, aggregate)
	db.PutEntity(destKey, &database.DataEntity{Data: ret})

	return protocol.MakeIntReply(ret.Len())
}

// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight
//  [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
func undoZUnionStore(db *DB, args [][]byte) []CmdLine {
	destKey := string(args[0])
	destSet, errReply := db.getAsSortedSet(destKey)
	if errReply != nil {
		return nil
	}
	// 这里分两种情况
	// 第一种，destination 不存在，所以回滚的话，直接 del，不论 destination 有没有
	if destSet == nil {
		return []CmdLine{
			utils.ToCmdLine3("DEL", args[0]),
		}
	}

	// 第二种，destination 存在，所以回滚的话，就重新按照原来的值 update 一遍
	// ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member...]
	var res []CmdLine
	destSet.ForEach(0, destSet.Len(), false, func(element *sortedset.Element) bool {
		scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
		res = append(res, utils.ToCmdLine2("ZADD", destKey, scoreStr, element.Member))
		return true
	})

	return res
}

// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight
//  [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
func execZInterStore(db *DB, args [][]byte) redis.Reply {
	destKey := string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal numkeys")
	}

	weights := make([]float64, numKeys)
	aggregate := utils.AGGREGATE_SUM


	weightsIndex := -1
	aggregateIndex := -1

	for i, arg := range args {
		argStr := strings.ToLower(string(arg))
		if argStr == "weights" {
			weightsIndex = i
		} else if argStr == "aggregate" {
			aggregateIndex = i
		} else if (argStr == "sum" || argStr == "min" || argStr == "max") && aggregateIndex == -1 {
			return protocol.MakeSyntaxErrReply()
		}
	}

	if weightsIndex != -1 {
		j := weightsIndex+1
		for i:=0;i<numKeys;i++ {
			w, err := strconv.ParseFloat(string(args[j]), 64)
			if err != nil {
				return protocol.MakeErrReply("illegal weight: " + string(args[j]))
			}
			weights[i] = w
			j++
		}
	} else {
		for i:=0;i<numKeys;i++ {
			weights[i] = 1
		}
	}

	if aggregateIndex != -1 {
		aggrOption := strings.ToLower(string(args[aggregateIndex+1]))
		if aggrOption == "min" {
			aggregate = utils.AGGREGATE_MIN
		} else if aggrOption == "max" {
			aggregate = utils.AGGREGATE_MAX
		}
	}

	sets := make([]*sortedset.SortedSet, numKeys)
	i := 2
	for j:=0;j<numKeys;j++ {
		key := string(args[i])
		sets[j], _ = db.getAsSortedSet(key)
		i++
	}
	logger.Info("weights: ", weights)
	ret := sortedset.Intersects(sets, weights, aggregate)

	db.PutEntity(destKey, &database.DataEntity{Data: ret})
	return protocol.MakeIntReply(ret.Len())
}

// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight
//  [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
func undoZInterStore(db *DB, args [][]byte) []CmdLine {
	return undoZUnionStore(db, args)
}

func init() {
	RegisterCommand("ZAdd", execZAdd, writeFirstKey, undoZAdd, -4, flagWrite)
	RegisterCommand("ZRange", execZRange, readFirstKey, nil, -4, flagReadOnly)
	RegisterCommand("ZRem", execZRem, writeFirstKey, undoZRem, -3, flagWrite)
	RegisterCommand("ZCard", execZCard, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("ZCount", execZCount, readFirstKey, nil, 4, flagReadOnly)
	RegisterCommand("ZRevRange", execZRevRange, readFirstKey, nil, -4, flagReadOnly)
	RegisterCommand("ZRank", execZRank, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("ZRevRank", execZRevRank, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("ZScore", execZScore, readFirstKey, nil, 3, flagReadOnly)
	RegisterCommand("ZDiffStore", execZDiffStore, readAllKeys, undoZDiffStore, -3, flagWrite)
	RegisterCommand("ZDiff", execZDiff, readAllKeys, nil, -3, flagReadOnly)
	RegisterCommand("ZIncrBy", execZIncrBy, writeFirstKey, undoZIncrBy, 4, flagWrite)
	RegisterCommand("ZUnionStore", execZUnionStore, writeFirstKey, undoZUnionStore, -4, flagWrite)
	RegisterCommand("ZInterStore", execZInterStore, writeFirstKey, undoZInterStore, -4, flagWrite)
}