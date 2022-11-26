package cluster

import (
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
)

func ping(cluster *Cluster, c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	return cluster.db.Exec(c, cmdLine)
}

/*
找出传入的 keys 中，每个 key 对应的节点
返回 peer -> keys 列表
 */
func (cluster *Cluster) groupBy(keys []string) map[string][]string {
	result := make(map[string][]string)

	for _, key := range keys {
		peer := cluster.peerPicker.PickNode(key)
		logger.Info("group by, key: ", key, "peer: ", peer)
		group, ok := result[peer]
		if !ok {
			group = make([]string, 0)
		}
		group = append(group, key)
		result[peer] = group
	}
	return result
}

func makeArgs(cmd string, args ...string) [][]byte {
	ret := make([][]byte, len(args) + 1)
	ret[0] = []byte(cmd)
	for i, arg := range args {
		ret[i+1] = []byte(arg)
	}
	return ret
}