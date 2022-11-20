package cluster

import (
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
)

func ping(cluster *Cluster, c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	return cluster.db.Exec(c, cmdLine)
}