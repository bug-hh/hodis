package cluster

import (
	"fmt"
	database2 "github.com/hodis/database"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/timewheel"
	"github.com/hodis/redis/protocol"
	"strconv"
	"strings"
	"sync"
	"time"
)

// prepareFunc executed after related key locked, and use additional logic to determine whether the transaction can be committed
// For example, prepareMSetNX  will return error to prevent MSetNx transaction from committing if any related key already exists
var prepareFuncMap = make(map[string]CmdFunc)

func registerPrepareFunc(cmdName string, fn CmdFunc) {
	prepareFuncMap[strings.ToLower(cmdName)] = fn
}

type Transaction struct {
	id string
	cmdLine [][]byte
	cluster *Cluster
	conn redis.Connection
	dbIndex int

	writeKeys []string
	readKeys []string

	keysLocked bool
	undoLog []database.CmdLine

	status int8
	mu *sync.Mutex
}

const (
	maxLockTime       = 3 * time.Second
	waitBeforeCleanTx = 2 * maxLockTime

	createdStatus    = 0
	preparedStatus   = 1
	committedStatus  = 2
	rolledBackStatus = 3
)

func genTaskKey(txID string) string {
	return "tx:" + txID
}

func NewTransaction(cluster *Cluster, c redis.Connection, id string, cmdLine [][]byte) *Transaction {
	return &Transaction{
		id:         id,
		cmdLine:    cmdLine,
		cluster:    cluster,
		conn:       c,
		dbIndex:    c.GetDBIndex(),
		status:     createdStatus,
		mu:         new(sync.Mutex),
	}
}

/*
// 锁定相关 key 避免并发问题
// 准备 undoLog
// 在时间轮中添加任务, 自动回滚超时未提交的事务
 */
func (tx *Transaction) prepare() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	//锁定相关 key 避免并发问题
	tx.writeKeys, tx.readKeys = database2.GetRelatedKeys(tx.cmdLine)
	tx.lockKeys()

	//准备 undo log
	tx.undoLog = tx.cluster.db.GetUndoLogs(tx.dbIndex, tx.cmdLine)
	tx.status = preparedStatus
	taskKey := genTaskKey(tx.id)

	//在时间轮中添加任务, 自动回滚超时未提交的事务
	timewheel.Delay(maxLockTime, taskKey, func() {
		if tx.status == preparedStatus {
			logger.Info("abort transaction: " + tx.id)
			_ = tx.rollBack()
		}
	})
	return nil
}

func (tx *Transaction) rollBack() error {
	curStatus := tx.status
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.status != curStatus {
		return fmt.Errorf("tx %s status changed", tx.id)
	}

	if tx.status == rolledBackStatus {
		return nil
	}
	tx.lockKeys()
	for _, cmdLine := range tx.undoLog {
		tx.cluster.db.ExecWithLock(tx.conn, cmdLine)
	}
	tx.unLockKeys()
	tx.status = rolledBackStatus
	return nil
}

// Reentrant
// invoker should hold tx.mu
func (tx *Transaction) lockKeys() {
	if !tx.keysLocked {
		tx.cluster.db.RWLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = true
	}
}

func (tx *Transaction) unLockKeys() {
	if tx.keysLocked {
		tx.cluster.db.RWUnLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = false
	}
}

// prepare 命令的格式： Prepare txID cmdName args
// 执行 prepare 阶段的工作
func execPrepare(cluster *Cluster, c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'prepare' command")
	}

	txID := string(cmdLine[1])
	cmdName := strings.ToLower(string(cmdLine[2]))

	tx := NewTransaction(cluster, c, txID, cmdLine[2:])
	cluster.transactions.Put(txID, tx)

	err := tx.prepare()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	prepareFunc, ok := prepareFuncMap[cmdName]
	if ok {
		return prepareFunc(cluster, c, cmdLine[2:])
	}
	return &protocol.OkReply{}
}

// requestRollback requests all node rollback transaction as coordinator
// groupMap: node -> keys
func requestRollback(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) {
	txIDStr := strconv.FormatInt(txID, 10)
	for node := range groupMap {
		if node == cluster.self {
			execRollback(cluster, c, makeArgs("rollback", txIDStr))
		} else {
			cluster.relay(node, c, makeArgs("rollback", txIDStr))
		}
	}
}

// execRollback rollbacks local transaction
func execRollback(cluster *Cluster, c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}
	txID := string(cmdLine[1])
	raw, ok := cluster.transactions.Get(txID)
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)
	err := tx.rollBack()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	// clean transaction
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactions.Remove(tx.id)
	})
	return protocol.MakeIntReply(1)
}

func execCommit(cluster *Cluster, c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}
	txID := string(cmdLine[1])
	raw, ok := cluster.transactions.Get(txID)
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	tx.mu.Lock()
	defer tx.mu.Unlock()

	result := cluster.db.ExecWithLock(c, tx.cmdLine)
	if protocol.IsErrorReply(result) {
		err2 := tx.rollBack()
		return protocol.MakeErrReply(fmt.Sprintf("err occurs when rollback: %v, origin err: %s", err2, result))
	}

	//after commited
	tx.unLockKeys()
	tx.status = committedStatus

	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactions.Remove(tx.id)
	})
	return result
}

func requestCommit(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) ([]redis.Reply, protocol.ErrorReply) {
	var errReply protocol.ErrorReply

	txIDStr := strconv.FormatInt(txID, 10)
	respList := make([]redis.Reply, 0, len(groupMap))
	for node := range groupMap {
		var resp redis.Reply
		if node == cluster.self {
			resp = execCommit(cluster, c, makeArgs("commit", txIDStr))
		} else {
			resp = cluster.relay(node, c, makeArgs("commit", txIDStr))
		}
		if protocol.IsErrorReply(resp) {
			errReply = resp.(protocol.ErrorReply)
			break
		}
		respList = append(respList, resp)
	}
	if errReply != nil {
		requestRollback(cluster, c, txID, groupMap)
		return nil, errReply
	}
	return respList, nil
}