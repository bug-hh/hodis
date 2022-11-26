package cluster

import (
	"bytes"
	"context"
	"errors"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/redis/client"
	"github.com/hodis/redis/protocol"
)

func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		logger.Info("getPeerClient: connection factory not found, peer: ", peer)
		return nil, errors.New("connection factory not found")
	}
	raw, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}

	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

/*
用完 client 后，再放回资源池
 */
func (cluster *Cluster) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		logger.Info("returnPeerClient: connection factory not found")
		return errors.New("connection factory not found")
	}
	return connectionFactory.ReturnObject(context.Background(), peerClient)
}

func CmdsToString(cmdLine database.CmdLine) string {
	bs := bytes.Buffer{}
	for _, cl := range cmdLine {
		bs.WriteString(string(cl))
		bs.WriteString(" ")
	}
	return bs.String()
}

var defaultRelayImpl = func(cluster *Cluster, node string, c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	cmd := CmdsToString(cmdLine)
	// 如果节点就是自己，那么就有自己来执行
	if node == cluster.self {
		logger.Info("自己执行: ", node, " 命令：", cmd)
		return cluster.db.Exec(c, cmdLine)
	}

	peerClient, err := cluster.getPeerClient(node)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(node, peerClient)
	}()
	logger.Info("peer 执行", peerClient.GetAddr(), "命令：", cmd)
	return peerClient.Send(cmdLine)
}

func (cluster *Cluster) relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.relayImpl(cluster, peer, c, args)
}