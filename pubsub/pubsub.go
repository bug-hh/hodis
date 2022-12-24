package pubsub

import (
	"github.com/hodis/datastruct/list"
	"github.com/hodis/interface/redis"
	"github.com/hodis/redis/protocol"
	"strconv"
)


var (
	_subscribe         = "subscribe"
	_unsubscribe       = "unsubscribe"
	messageBytes       = []byte("message")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

func makeMsg(t string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + protocol.CRLF + t + protocol.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 10) + protocol.CRLF + channel + protocol.CRLF +
		":" + strconv.FormatInt(code, 10) + protocol.CRLF)
}

func Subscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, b := range args {
		channels[i] = string(b)
	}

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		if subscribe0(hub, channel, c) {
			_ = c.Write(makeMsg(_subscribe, channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

func subscribe0(hub *Hub, channel string, client redis.Connection) bool {
	client.Subscribe(channel)

	raw, ok := hub.subs.Get(channel)
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.subs.Put(channel, subscribers)
	}
	if subscribers.Contains(func(a interface{}) bool {
		return a == client
	}) {
		return false
	}
	subscribers.Add(client)
	return true
}

// 给所有订阅了 channel 的 client 发消息
func Publish(hub *Hub, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return &protocol.ArgNumErrReply{
			Cmd: "publish",
		}
	}

	channel := string(args[0])
	message := args[1]

	hub.subsLocker.Locks(channel)
	defer hub.subsLocker.UnLocks(channel)

	raw, ok := hub.subs.Get(channel)
	if !ok {
		return protocol.MakeIntReply(0)
	}
	subscribers, _ := raw.(*list.LinkedList)
	subscribers.ForEach(func(i int, v interface{}) bool {
		client, _ := v.(redis.Connection)
		replyArgs := make([][]byte, 3)
		replyArgs[0] = messageBytes
		replyArgs[1] = []byte(channel)
		replyArgs[2] = message
		_ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
		return true
	})
	return protocol.MakeIntReply(int64(subscribers.Len()))

}