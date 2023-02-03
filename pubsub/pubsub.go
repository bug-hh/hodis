package pubsub

import (
	"github.com/hodis/datastruct/list"
	"github.com/hodis/interface/redis"
	"github.com/hodis/redis/connection"
	"github.com/hodis/redis/protocol"
	"regexp"
	"strconv"
)


var (
	_subscribe         = "subscribe"
	_psubscribe         = "psubscribe"
	_unsubscribe       = "unsubscribe"
	messageBytes       = []byte("message")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

func makeMsg(t string, channel string, code int64) []byte {
	bs := [][]byte{
		[]byte(t),
		[]byte(channel),
		[]byte(strconv.FormatInt(code, 10)),
	}
	return protocol.MakeMultiBulkReply(bs).ToBytes()
	//return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + protocol.CRLF + t + protocol.CRLF +
	//	"$" + strconv.FormatInt(int64(len(channel)), 10) + protocol.CRLF + channel + protocol.CRLF +
	//	":" + strconv.FormatInt(code, 10) + protocol.CRLF)
}

func PSubscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, b := range args {
		channels[i] = string(b)
	}

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		if psubscribe0(hub, channel, c) {
			_ = c.Write(makeMsg(_psubscribe, channel, int64(c.PSubsCount())))
		}
	}
	return &protocol.NoReply{}
}

func Subscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, b := range args {
		channels[i] = string(b)
	}
	// todo 并发锁应该看看
	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		if subscribe0(hub, channel, c) {
			_ = c.Write(makeMsg(_subscribe, channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

func psubscribe0(hub *Hub, channel string, client redis.Connection) bool {
	client.PSubscribe(channel)

	raw, ok := hub.patternSubs.Get(channel)
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.patternSubs.Put(channel, subscribers)
	}

	if subscribers.Contains(func(a interface{}) bool {
		return a == client
	}) {
		return false
	}
	subscribers.Add(client)
	return true
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
	count := 0
	hub.subsLocker.Locks(channel)
	defer hub.subsLocker.UnLocks(channel)

	// 给订阅了 channel 的 client 发消息
	raw, ok := hub.subs.Get(channel)
	subscribers, _ := raw.(*list.LinkedList)
	if ok {
		subscribers.ForEach(func(i int, v interface{}) bool {
			client, _ := v.(redis.Connection)
			replyArgs := make([][]byte, 3)
			replyArgs[0] = messageBytes
			replyArgs[1] = []byte(channel)
			replyArgs[2] = message
			_ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
			return true
		})
		count += subscribers.Len()
	}

	// 给订阅了 pattern 且 pattern 匹配 channel 的 client 发消息
	// 先找出能够匹配 channel 的 pattern
	patterns := hub.patternSubs.Keys()

	for _, p := range patterns {
		reg, err := regexp.Compile(p)
		if err != nil {
			continue
		}
		if reg.MatchString(channel) {
			// 找到 pattern 对应的 client
			raw, ok = hub.patternSubs.Get(p)
			if ok {
				psubscribers, _ := raw.(*list.LinkedList)
				psubscribers.ForEach(func(i int, v interface{}) bool {
					client, _ := v.(redis.Connection)
					replyArgs := make([][]byte, 3)
					replyArgs[0] = messageBytes
					replyArgs[1] = []byte(channel)
					replyArgs[2] = message
					_ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
					return true
				})
				count += psubscribers.Len()
			}
		}
	}

	return protocol.MakeIntReply(int64(count))
}

func Unsubscribe(hub *Hub, client redis.Connection, args [][]byte) redis.Reply {
	// 把 client 订阅的所有 channel 都取消掉
	conn, _ := client.(*connection.Connection)
	if args == nil || len(args) == 0 {
		hub.subs.ForEach(func(key string, val interface{}) bool {
			subcribers, _ := val.(*list.LinkedList)
			subcribers.Remove(subcribers.Get(func(a interface{}) bool {
				clientItem, _ := a.(*connection.Connection)
				return clientItem.RemoteAddr().String() == conn.RemoteAddr().String()
			}))
			return true
		})
	} else {
		channels := make([]string, 0, len(args))
		for _, c := range args {
			channels = append(channels, string(c))
		}
		for _, channel := range channels {
			raw, exists := hub.subs.Get(channel)
			if !exists {
				continue
			}
			subcribers, _ := raw.(*list.LinkedList)
			subcribers.Remove(subcribers.Get(func(a interface{}) bool {
				clientItem, _ := a.(*connection.Connection)
				return clientItem.RemoteAddr().String() == conn.RemoteAddr().String()
			}))
		}
	}
	return protocol.MakeOkReply()
}

func PUnsubscribe(hub *Hub, client redis.Connection, args [][]byte) redis.Reply {
	// 把 client 订阅的所有 channel 都取消掉
	conn, _ := client.(*connection.Connection)
	if len(args) == 0 {
		hub.patternSubs.ForEach(func(key string, val interface{}) bool {
			subcribers, _ := val.(*list.LinkedList)
			subcribers.Remove(subcribers.Get(func(a interface{}) bool {
				clientItem, _ := a.(*connection.Connection)
				return clientItem.RemoteAddr().String() == conn.RemoteAddr().String()
			}))
			return true
		})
	} else {
		channels := make([]string, 0, len(args))
		for _, c := range args {
			channels = append(channels, string(c))
		}
		for _, channel := range channels {
			raw, exists := hub.patternSubs.Get(channel)
			if !exists {
				continue
			}
			subcribers, _ := raw.(*list.LinkedList)
			subcribers.Remove(subcribers.Get(func(a interface{}) bool {
				clientItem, _ := a.(*connection.Connection)
				return clientItem.RemoteAddr().String() == conn.RemoteAddr().String()
			}))
		}
	}
	return protocol.MakeOkReply()
}