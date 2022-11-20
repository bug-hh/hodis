package cluster

import (
	"context"
	"fmt"
	"github.com/hodis/config"
	database2 "github.com/hodis/database"
	"github.com/hodis/datastruct/dict"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/consistenthash"
	"github.com/hodis/lib/idgenerator"
	"github.com/hodis/lib/logger"
	"github.com/hodis/redis/protocol"
	"github.com/jolestar/go-commons-pool/v2"
	"runtime/debug"
	"strings"
)

type PeerPicker interface {
	AddNode(keys ...string)
	PickNode(key string) string
}

/*
用来定义集群中的某个节点
 */
type Cluster struct {
	self string

	nodes []string
	peerPicker PeerPicker
	peerConnection map[string]*pool.ObjectPool

	db database.EmbedDB
	transactions *dict.SimpleDict  // id -> Transaction

	idGenerator *idgenerator.IDGenerator

	// use a variable to allow injecting stub for testing
	relayImpl func(cluster *Cluster, node string, c redis.Connection, cmdLine database.CmdLine) redis.Reply
}

const (
	replicas = 4
)

var allowFastTransaction = true

func MakeCluster() *Cluster {
	cluster := &Cluster{
		self: config.Properties.Self,
		db: database2.NewStandaloneServer(),
		transactions: dict.MakeSimple(),
		peerPicker: consistenthash.New(replicas, nil),
		peerConnection: make(map[string]*pool.ObjectPool),

		idGenerator: idgenerator.MakeGenerator(config.Properties.Self),
		relayImpl: defaultRelayImpl,
	}

	contains := make(map[string]struct{})
	nodes := make([]string, 0, len(config.Properties.Peers) + 1)
	/*
	根据配置文件，在当前节点启动时，添加集群中的其他节点
	原作者没有实现 cluster node 和 cluster meet 命令，
	也就是说，没法实时查看和实时添加节点
	 */
	for _, peer := range config.Properties.Peers {
		if _, ok := contains[peer]; ok {
			continue
		}
		contains[peer] = struct{}{}
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	logger.Info("peerConnection: ", len(cluster.peerConnection), cluster.peerConnection)
	cluster.nodes = nodes
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine database.CmdLine) redis.Reply

// Close stops current node of cluster
func (cluster *Cluster) Close() {
	cluster.db.Close()
}

var router = makeRouter()

// 在 redis 集群上执行命令
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "auth" {
		// todo 以后实现鉴权

	}

	// todo 以后实现鉴权

	// todo 以后实现执行特殊命令
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}


	} else if cmdName == "discard" {

	} else if cmdName == "exec" {

	} else if cmdName == "select" {

	} else if cmdName == "cluster" {
		/*
		原作者并没有实现 cluster nodes，，cluster info，cluster meet 这种命令
		 */
		if len(cmdLine) == 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return cluster.getClusterInfo(c, cmdLine)
	}

	cmdFunc, ok := router[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}

	result = cmdFunc(cluster, c, cmdLine)
	return
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}

/*
用来实现 cluster nodes, cluster meet, cluster info 命令
 */
func (cluster *Cluster) getClusterInfo(c redis.Connection, args [][]byte) redis.Reply {
	cmd := string(args[1])
	if cmd == "nodes" {
		// 返回这个集群中的所有节点信息，这里简化处理，直接放回节点 ip:port
		ret := make([][]byte, 0, len(cluster.nodes))
		for _, item := range cluster.nodes {
			ret = append(ret, []byte(item))
		}
		return protocol.MakeMultiBulkReply(ret)
	} else {
		// todo 以后实现 cluster meet 和 cluster info
		return protocol.MakeBulkReply([]byte("coming soon"))
	}

}