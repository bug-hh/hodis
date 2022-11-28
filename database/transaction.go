package database

import (
	"fmt"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/redis/protocol"
	"strings"
)

func (db *DB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	writeKeys := make([]string, 0)
	readKeys := make([]string, 0)

	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]
		prepare := cmd.prepare
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}

	// set watch
	watchingKeys := make([]string, 0, len(watching))

	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}

	readKeys = append(readKeys, watchingKeys...)

	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)

	// 当监视的 key 已经发生了改变，那么立刻终止当前事务
	if isWatchingChanged(db, watching) {
		return protocol.MakeEmptyMultiBulkReply()
	}

	// 开始执行事务
	logger.Info("开始执行事务")
	results := make([]redis.Reply, 0, len(cmdLines))
	aborted := false
	undoCmdLines := make([][]CmdLine, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		logger.Info("执行: ", cmdName)
		undoCmdLines = append(undoCmdLines, db.GetUndoLogs(cmdLine))
		result := db.execWithLock(cmdLine)
		logger.Info("执行结果", string(result.ToBytes()))
		if protocol.IsErrorReply(result) {
			aborted = true
			// don't rollback failed commands
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			break
		}
		results = append(results, result)
	}
	logger.Info("事务执行完毕，事务包含: ", len(results), "结果")
	// 事务中所有命令都执行成功了
	if !aborted {
		db.addVersion(writeKeys...)
		ret := protocol.MakeMultiRawReply(results)
		bs := ret.ToBytes()
		fmt.Printf("ExecMulti: ")
		for _, item := range bs {
			fmt.Printf("%02d, ", item)
		}
		fmt.Println()
		return ret
	}

	// undo if aborted
	size := len(undoCmdLines)
	for i := size - 1; i >= 0; i-- {
		curCmdLines := undoCmdLines[i]
		if len(curCmdLines) == 0 {
			continue
		}
		for _, cmdLine := range curCmdLines {
			db.execWithLock(cmdLine)
		}
	}
	return protocol.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
}

/*
Redis的事务和传统的关系型数据库事务的最大区别在于，
Redis不支持事务回滚机制(rollback),即使事务队列中的某个命令在执行期间出现了错误，
整个事务也会继续执行下去，直到将事务队列中的所有命令都执行完毕为止。
 */
func (db *DB) GetUndoLogs(cmdLine [][]byte) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.undo
	if undo == nil {
		return nil
	}
	return undo(db, cmdLine[1:])
}

func DiscardMulti(conn redis.Connection) redis.Reply {
	// 如果不是在一个事务里执行 discard，就报错
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR DISCARD without MULTI")
	}
	conn.ClearQueuedCmds()
	conn.SetMultiState(false)
	return protocol.MakeOkReply()
}

func Watch(db *DB, conn redis.Connection, args [][]byte) redis.Reply {
	watching := conn.GetWatching()
	for _, bkey := range args {
		key := string(bkey)
		watching[key] = db.GetVersion(key)
	}
	return protocol.MakeOkReply()
}

func isWatchingChanged(db *DB, watching map[string]uint32) bool {
	for key, ver := range watching {
		currentVersion := db.GetVersion(key)
		if ver != currentVersion {
			return true
		}
	}
	return false
}

// GetRelatedKeys analysis related keys
func GetRelatedKeys(cmdLine [][]byte) ([]string, []string) {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil, nil
	}
	prepare := cmd.prepare
	if prepare == nil {
		return nil, nil
	}
	return prepare(cmdLine[1:])
}

func StartMulti(conn redis.Connection) redis.Reply {
	if conn.InMultiState() {
		return protocol.MakeErrReply("ERR MULTI calls can not be nested")
	}
	conn.SetMultiState(true)
	return protocol.MakeOkReply()
}

func EnqueueCmd(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		err := protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
		conn.AddTxError(err)
		return err
	}
	/*
	检查有没有 prepare 函数，prepare 函数用于给 key 加锁、构建 undo log
	没有 prepare 函数的命令，不能用于事务
	 */
	if cmd.prepare == nil {
		err := protocol.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
		conn.AddTxError(err)
		return err
	}
	/*
	检查参数个数是否合法
	 */
	if !validateArity(cmd.arity, cmdLine) {
		err := protocol.MakeArgNumErrReply(cmdName)
		conn.AddTxError(err)
		return err
	}

	conn.EnqueueCmd(cmdLine)
	return protocol.MakeQueuedReply()
}

func execMulti(db *DB, conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	if len(conn.GetTxErrors()) > 0 {
		return protocol.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
	}
	cmdLines := conn.GetQueuedCmdLine()
	return db.ExecMulti(conn, conn.GetWatching(), cmdLines)
}