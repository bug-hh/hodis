package cluster

import (
	"fmt"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/protocol"
	"strconv"
)

const keyExistsErr = "key exists"
/*
set k1 v1
set k2 v2
set key2 value2
set job apple
 */


func MGet(cluster *Cluster, c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	logger.Info("MGet, cmdLine: ", CmdsToString(cmdLine))
	if len(cmdLine) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'mget' command")
	}

	keys := make([]string, len(cmdLine)-1)
	for i:=1;i<len(cmdLine);i++ {
		keys[i-1] = string(cmdLine[i])
	}

	resultMap := make(map[string][]byte)

	groupMap := cluster.groupBy(keys)
	logger.Info("MGet, keys: ", keys)
	for peer, keyList := range groupMap {
		logger.Info("peer: ", peer, " -> ", keyList)
	}

	for peer, group := range groupMap {
		resp := cluster.relay(peer, c, makeArgs("MGet", group...))
		logger.Info("MGet resp: ", string(resp.ToBytes()))
		if protocol.IsErrorReply(resp) {
			errReply := resp.(protocol.ErrorReply)
			return protocol.MakeErrReply(fmt.Sprintf("ERR during get %s occurs: %v", group[0], errReply.Error()))
		}
		arrReply, _ := resp.(*protocol.MultiBulkReply)
		//arrReply.Args[i] 存的就是第 i 个 key 对应的 value
		for i, v := range arrReply.Args {
			key := group[i]
			resultMap[key] = v
		}
	}
	result := make([][]byte, len(keys))
	for i, k := range keys {
		result[i] = resultMap[k]
	}
	return protocol.MakeMultiBulkReply(result)
}

/*
集群模式先的 mset 命令，还需要实现单机下的 mset
 */
func MSet(cluster *Cluster, c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	argCount := len(cmdLine) -1
	if argCount % 2 != 0 || argCount < 1 {
		return protocol.MakeErrReply("wrong number of arguments for 'mset' command")
	}

	size := argCount / 2
	keys := make([]string, size)
	valueMap := make(map[string]string)

	for i:=0;i<size;i++ {
		keys[i] = string(cmdLine[2*i+1])
		valueMap[keys[i]] = string(cmdLine[2*i+2])
	}

	groupMap := cluster.groupBy(keys)
	// 如果所有的 key 都在同一个节点上，那么直接发起命令，不走 TCC（try commit catch)
	if len(groupMap) == 1 && allowFastTransaction {
		for peer := range groupMap {
			return cluster.relay(peer, c, cmdLine)
		}
	}

	var errReply redis.Reply

	txID := cluster.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	rollBack := false

	for peer, group := range groupMap {
		peerArgs := []string{txIDStr, "MSET"}
		for _, k := range group {
			peerArgs = append(peerArgs, k, valueMap[k])
		}
		var resp redis.Reply

		// 执行 prepare 阶段
		if peer == cluster.self {
			resp = execPrepare(cluster, c, makeArgs("Prepare", peerArgs...))
		} else {
			resp = cluster.relay(peer, c, makeArgs("Prepare", peerArgs...))
		}

		if protocol.IsErrorReply(resp) {
			errReply = resp
			rollBack = true
			break
		}
	}

	if rollBack {
		requestRollback(cluster, c, txID, groupMap)
	} else {
		_, errReply = requestCommit(cluster, c, txID, groupMap)
		rollBack = errReply != nil
	}

	if !rollBack {
		return &protocol.OkReply{}
	}
	return errReply
}

func prepareMSetNx(cluster *Cluster, conn redis.Connection, cmdLine database.CmdLine) redis.Reply {
	args := cmdLine[1:]
	if len(args) % 2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}

	size := len(args) / 2
	values := make([][]byte, size)
	keys := make([]string, size)

	for i:=0;i<size;i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}
	resp := cluster.db.ExecWithLock(conn, utils.ToCmdLine2("ExistIn", keys...))
	if protocol.IsErrorReply(resp) {
		return resp
	}
	_, ok := resp.(*protocol.EmptyMultiBulkReply)
	if !ok {
		return protocol.MakeErrReply(keyExistsErr)
	}
	return protocol.MakeOkReply()
}

func init() {
	// mset 命令，同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在。
	registerPrepareFunc("MSetNx", prepareMSetNx)

}