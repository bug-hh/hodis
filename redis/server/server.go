package server

import (
	"context"
	"github.com/hodis/config"
	database2 "github.com/hodis/database"
	"github.com/hodis/interface/database"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/sync/atomic"
	"github.com/hodis/redis/connection"
	"github.com/hodis/redis/parser"
	"github.com/hodis/redis/protocol"
	"io"
	"net"
	"strings"
	"sync"
)

// Handler implements tcp.Handler and serves as a redis server
type Handler struct {
	activeConn sync.Map
	db database.DB
	closing atomic.Boolean
}

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

func MakeHandler() *Handler {
	var db database.DB

	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		// 开启集群模式
	} else {
		logger.Info("开启单机模式")
		// 开启单机模式
		db = database2.NewStandaloneServer()
	}
	return &Handler{
		db: db,
	}
}

// Handle receives and executes redis commands
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, 1)
	logger.Info("开始解析 stream")
	ch := parser.ParseStream(conn)
	logger.Info("解析完成")
	for payload := range ch {
		if payload.Err != nil {
			// 如果遇到的是 EOF 错误，那么就关闭客户端连接
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err
			errReply := protocol.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		/*
			这个 Data 字段是 Reply 类型，
			而这个 Reply类型是个 interface
			只要括号内的类型实现了 Reply 里的方法，就能进行类型转换
		*/
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		logger.Info("开始执行命令")
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Close stops handler
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}