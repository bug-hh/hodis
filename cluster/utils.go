package cluster

import (
	"github.com/hodis/config"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/redis/protocol"
	"strconv"
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

func execSelect(c redis.Connection, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DB index")
	}

	if dbIndex >= config.Properties.Databases || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}