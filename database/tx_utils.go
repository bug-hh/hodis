package database

import (
	"github.com/hodis/aof"
	"github.com/hodis/lib/utils"
	"strconv"
)



func writeAllKeys(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return keys, nil
}

func readAllKeys(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}

func readFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return nil, []string{key}
}

func writeFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func rollbackFirstKey(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return rollbackGivenKeys(db, key)
}

func rollbackGivenKeys(db *DB, keys ...string) []CmdLine {
	var undoCmdLines [][][]byte
	for _, key := range keys {
		entity, ok := db.GetEntity(key)
		if !ok {
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("DEL", key))
		} else {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("DEL", key),
				aof.EntityToCmd(key, entity).Args,
				toTTLCmd(db, key).Args,
			)
		}
	}
	return undoCmdLines
}

func rollbackZSetFields(db *DB, key string, fields ...string) []CmdLine {
	var undoCmdLines [][][]byte
	zset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return nil
	}

	if zset == nil {
		undoCmdLines = append(undoCmdLines, utils.ToCmdLine("DEL", key))
		return undoCmdLines
	}

	for _, field := range fields {
		elem, ok := zset.Get(key)
		if !ok {
			/*
			Zrem 命令用于移除有序集中的一个或多个成员，不存在的成员将被忽略。
			当 key 存在但不是有序集类型时，返回一个错误。
			 */
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("ZREM", key, field))
		} else {
			score := strconv.FormatFloat(elem.Score, 'f', -1, 64)
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("ZADD", key, score, field))
		}
	}
	return undoCmdLines
}

func noPrepare(args [][]byte) ([]string, []string) {
	return nil, nil
}