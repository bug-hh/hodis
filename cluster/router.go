package cluster

import (
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
)

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["ping"] = ping

	routerMap["prepare"] = execPrepare
	routerMap["commit"] = execCommit
	routerMap["rollback"] = execRollback

	// string
	routerMap["set"] = defaultFunc
	routerMap["get"] = defaultFunc

	routerMap["mset"] = MSet
	routerMap["mget"] = MGet


	// sortedset
	routerMap["zadd"] = defaultFunc
	routerMap["zrange"] = defaultFunc
	routerMap["zrem"] = defaultFunc

	routerMap["lpush"] = defaultFunc
	routerMap["rpush"] = defaultFunc
	routerMap["lrange"] = defaultFunc
	routerMap["rpop"] = defaultFunc

	return routerMap
}


/*
把命令传给对应的（同集群）节点执行
 */
func defaultFunc(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	key := string(args[1])
	// 先按照 key 选择集群中对应的节点
	logger.Info("调 PickNode")
	peer := cluster.peerPicker.PickNode(key)
	logger.Info("defaultFunc: peer: ", peer, "key: ", key)
	// 然后把命令传到指定节点执行
	return cluster.relay(peer, c, args)
}