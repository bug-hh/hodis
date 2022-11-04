package database

import (
	"github.com/hodis/aof"
	"github.com/hodis/lib/utils"
)

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