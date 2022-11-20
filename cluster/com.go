package cluster

import (
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

var defaultRelayImpl = func(cluster *Cluster, node string, c redis.Connection, cmdLine database.CmdLine) redis.Reply {
	// 如果节点就是自己，那么就有自己来执行
	if node == cluster.self {
		return cluster.db.Exec(c, cmdLine)
	}

	peerClient, err := cluster.getPeerClient(node)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(node, peerClient)
	}()
	return peerClient.Send(cmdLine)
}

func (cluster *Cluster) relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.relayImpl(cluster, peer, c, args)
}