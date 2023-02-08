package aof

import (
	"github.com/hodis/config"
	"github.com/hodis/interface/database"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/connection"
	"github.com/hodis/redis/parser"
	"github.com/hodis/redis/protocol"
	"io"
	"os"
	"strconv"
	"sync"
)

type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdline CmdLine
	dbIndex int
}

type Handler struct {
	db         database.EmbedDB
	tmpDBMaker func() database.EmbedDB
	// 主线程使用此channel将要持久化的命令发送到异步协程
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string

	aofFinished chan struct{}

	// pause aof for start/finish aof rewrite progress
	pausingAof sync.RWMutex
	currentDB int
}

func NewAOFHandler(db database.EmbedDB, replFunc func(cmdLine [][]byte, index int), tmpDBMaker func() database.EmbedDB) (*Handler, error) {
	handler := &Handler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = db
	handler.tmpDBMaker = tmpDBMaker
	// 读取已有的 aof 文件, 节点初始化，执行 aof 文件里的命令时，不会再次写入这些命令到 aof 文件中，
	// 因为当前处理 aof 的协程没有启动，管道也没有初始化
	handler.LoadAof(0, replFunc)
	// 打开这个 aof 文件，获取文件描述符
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND | os.O_CREATE | os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	handler.aofFinished = make(chan struct{})

	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

func (handler *Handler) handleAof() {
	handler.currentDB = 0
	// 协程从 channel 里读主协程写入的命令
	for p := range handler.aofChan {
		// 使用锁保证每次都会写入一条完整的命令
		handler.pausingAof.RLock()
		if p.dbIndex != handler.currentDB {
			// select db
			data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}
		data := protocol.MakeMultiBulkReply(p.cmdline).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
		handler.pausingAof.RUnlock()
	}
	handler.aofFinished <- struct{}{}
}

// read aof file
func (handler *Handler) LoadAof(maxBytes int, replFunc func(cmdLine [][]byte, index int)) {
	// delete aofChan to prevent write again
	/*
	删除这个 aofChan 的原因是，当存在 aofChan 时，主协程会往里写，然后从协程从里面读，然后往
	aofFile 里面写，而这个函数正在读 aofFile，如果不删除 aofChan,会造成 一个不停读，一个不停写
	 */
	aofChan := handler.aofChan
	handler.aofChan = nil
	defer func(aofChan chan *payload) {
		handler.aofChan = aofChan
	}(aofChan)

	if !utils.PathExists(handler.aofFilename) {
		return
	}
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			logger.Warn("load aof file path error: ", handler.aofFilename)
			return
		}
		logger.Warn(err)
		return
	}

	defer file.Close()

	var reader io.Reader

	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}

	ch := parser.ParseStream(reader)
	// 创建一个伪客户端来执行 aof 文件里的命令
	fakeConn := &connection.FakeConn{}
	i := 0
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}

		// p.Data 是个 redis.Reply 类型，这是一个 interface 类型
		// 而 protocol.MultiBulkReply 实现了这个接口
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}

		// 如果是一个 master，需要填充复制缓冲区
		replFunc(r.Args, i)
		i++

		ret := handler.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", ret.ToBytes())
		}
	}
}

func (handler *Handler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdline: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

func (handler *Handler) Close() {
	if handler.aofFile != nil {
		close(handler.aofChan)
		<-handler.aofFinished
		err := handler.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
}