package client

import (
	"errors"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/sync/wait"
	"github.com/hodis/redis/parser"
	"github.com/hodis/redis/protocol"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	running
	closed
)

type Client struct {
	conn net.Conn
	pendingReqs chan *request  // wait to send
	waitingReqs chan *request  // waiting response
	/*
	因为作者原有的方案，Send 返回的 result 总为空，
	所以在原来作者的基础上增加 finishReqs，用来存放服务器已完成的请求
	客户端直接从这个 channel 里取结果
	 */
	finishReqs chan *request
	ticker *time.Ticker
	addr string

	status int32
	working *sync.WaitGroup  // its counter presents unfinished requests(pending and waiting)
}

type request struct {
	id uint64
	args [][]byte
	reply redis.Reply
	heartbeat bool
	waiting *wait.Wait
	err error
}

const (
	chanSize = 256
	maxWait = 3*time.Second
)

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		finishReqs: make(chan *request, chanSize),
		addr:        addr,
		working:     &sync.WaitGroup{},
	}, nil
}

// Send sends a request to redis server
func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrReply("client closed")
	}
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- req
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		logger.Info("Send: server time out")
		return protocol.MakeErrReply("server time out")
	}
	if req.err != nil {
		logger.Info("Send: request failed")
		return protocol.MakeErrReply("request failed")
	}
	// 取结果
	ret := <- client.finishReqs
	return ret.reply
}

func (client *Client) Start() {
	client.ticker = time.NewTicker(10*time.Second)
	go client.handleWrite()
	go client.handleRead()
	go client.heartbeat()
	atomic.StoreInt32(&client.status, running)
}

func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	client.ticker.Stop()

	// 不再处理新的请求
	close(client.pendingReqs)
	// 等待所有请求处理完
	client.working.Wait()

	_ = client.conn.Close()

	//关闭读请求
	close(client.waitingReqs)

	// 关闭读取结果 channel
	close(client.finishReqs)
}

func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr)
	_ = client.conn.Close()

	var conn net.Conn
	// 重新连接重试 3 次
	for i:=0;i<3;i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconenct error: ", client.addr)
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	if conn == nil {
		client.Close()
		return
	}

	client.conn = conn
	close(client.waitingReqs)

	for req := range client.waitingReqs {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}
	client.waitingReqs = make(chan *request, chanSize)
	// restart handle read
	go client.handleRead()
}

func (client *Client) heartbeat() {
	// 每隔 10s 发一次心跳包
	for range client.ticker.C {
		client.doHeartbeat()
	}
}
/*
用于处理来自客户端的请求
 */
func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()

	req := <- client.waitingReqs
	if req == nil {
		return
	}
	req.reply = reply
	/*
	只把 非 ping 的请求结果放入 channel
	因为 ping 每 10s 就发一次，如果把它加入，那么会搞乱其他客户端发送的请求
	 */
	if !req.heartbeat {
		client.finishReqs <- req
	}

	if req.waiting != nil {
		req.waiting.Done()
	}
}

// 读取服务端发来的 response
func (client *Client) handleRead() {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			logger.Debug("payload err: ", payload.Err.Error())
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}

}

func (client *Client) doHeartbeat() {
	req := &request{
		args: [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting: &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- req
	req.waiting.WaitWithTimeout(maxWait)
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}

	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	var err error
	// 如果发生错误，会重试 3 次，但是超时不重试
	for i:=0;i<3;i++ {
		_, err = client.conn.Write(bytes)
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") &&
				!strings.Contains(err.Error(), "deadline exceed")) {
			break
		}
	}
	if err == nil {
		// 加入到等待服务端响应的 channel 中
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

