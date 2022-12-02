package cluster

import (
	database2 "github.com/hodis/database"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/protocol"
	"strconv"
)

const relayMulti = "_multi"
const innerWatch = "_watch"

var relayMultiBytes = []byte(relayMulti)

func execMulti(cluster *Cluster, conn redis.Connection, cmdLine database.CmdLine) redis.Reply {
	// 首先判断是否在一个事务内
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR EXEC without multi")
	}
	defer conn.SetMultiState(false)

	// 获取事务内所有的命令
	cmdLines := conn.GetQueuedCmdLine()

	// 获取执行事务所用到的所有 key
	keys := make([]string, 0)
	for _, cl := range cmdLines {
		wKeys, rKeys := database2.GetRelatedKeys(cl)
		keys = append(keys, wKeys...)
		keys = append(keys, rKeys...)
	}
	watching := conn.GetWatching()
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}

	keys = append(keys, watchingKeys...)
	if len(keys) == 0 {
		// empty transaction or only `PING`s
		return cluster.db.ExecMulti(conn, watching, cmdLines)
	}

	/*
	找到每个 key 对应的 节点
	peer -> [keys]
	 */
	groupMap := cluster.groupBy(keys)
	/*
	一个事务有多条命令，这些命令涉及的 key 必须在同一个节点上
	 */
	// todo 有个问题，redis 是否支持一个事务中横跨多个节点执行？
	// 实际测试来看，redis 不支持在一个事务中横跨多个节点执行，会报错
	if len(groupMap) > 1 {
		return protocol.MakeErrReply("必须在一个节点上执行事务")
	}
	var peer string

	for p := range groupMap {
		peer = p
	}
	// 就在本地节点执行
	if peer == cluster.self {
		return cluster.db.ExecMulti(conn, watching, cmdLines)
	}

	return execMultiOnOtherNode(cluster, conn, peer, watching, cmdLines)
}

func execMultiOnOtherNode(cluster *Cluster, conn redis.Connection, peer string, watching map[string]uint32, cmdLines []database.CmdLine) redis.Reply {
	defer func() {
		conn.ClearQueuedCmds()
		conn.SetMultiState(false)
	}()

	relayCmdLine := [][]byte{
		relayMultiBytes,
	}

	var watchingCmdLine = utils.ToCmdLine(innerWatch)
	for key, ver := range watching {
		verStr := strconv.FormatUint(uint64(ver), 10)
		watchingCmdLine = append(watchingCmdLine, []byte(key), []byte(verStr))
	}

	// 这里为什么要 base64 编码呢？
	relayCmdLine = append(relayCmdLine, encodeCmdLine([]database.CmdLine{watchingCmdLine})...)
	relayCmdLine = append(relayCmdLine, encodeCmdLine(cmdLines)...)

	rawRelayResult := cluster.relay(peer, conn, relayCmdLine)

	if protocol.IsErrorReply(rawRelayResult) {
		return rawRelayResult
	}

	_, ok := rawRelayResult.(*protocol.EmptyMultiBulkReply)
	if ok {
		return rawRelayResult
	}

	relayResult, ok := rawRelayResult.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("execute failed")
	}

	resp, err := parseEncodedMultiRawReply(relayResult.Args)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return resp
}
