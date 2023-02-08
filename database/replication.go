package database

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	rdb "github.com/hdt3213/rdb/parser"
	"github.com/hodis/config"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/connection"
	"github.com/hodis/redis/parser"
	"github.com/hodis/redis/protocol"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MasterRole = iota
	SlaveRole
	SentinelRole
)

const (
	REPL_BUFFER_SIZE = 1024 * 1024
)

type slaveNode struct {
	slaveIp string
	slavePort string
}

type replicationStatus struct {
	mutex sync.Mutex
	ctx context.Context
	cancel context.CancelFunc
	modCount int32  // execSlaveOf() or reconnect will cause modCount++

	masterHost string
	masterPort int

	masterConn net.Conn
	masterChan <-chan *parser.Payload

	/*
	专门用于主从之间收到心跳检测包的通道
	 */
	masterAckConn net.Conn
	masterAckChan <-chan *parser.Payload

	replId string
	replOffset int64
	replOffsetFileName string

	lastRecvTime time.Time
	running sync.WaitGroup

	isMasterOnline bool
	isRDBSyncDone bool

	// 复制缓冲区
	replBuffer [][][]byte
}

func (repl *replicationStatus) GetReplBuf() [][][]byte {
	return repl.replBuffer
}

func initReplStatus() *replicationStatus {
	repl := &replicationStatus{
		replOffsetFileName: fmt.Sprintf("repl_offset_%s_%d.conf", config.Properties.Bind, config.Properties.Port),
		replBuffer: make([][][]byte, REPL_BUFFER_SIZE),
	}
	readReplConf(repl)
	return repl
}

func (repl *replicationStatus) sendAck2Master() error {
	repl.mutex.Lock()
	defer repl.mutex.Unlock()
	psyncCmdLine := utils.ToCmdLine("REPLCONF", "ACK",
		strconv.FormatInt(repl.replOffset, 10))
	psyncReq := protocol.MakeMultiBulkReply(psyncCmdLine)
	//_, err := repl.masterConn.Write(psyncReq.ToBytes())
	_, err := repl.masterAckConn.Write(psyncReq.ToBytes())
	return err
}

func (mdb *MultiDB) reconnectWithMaster() error {
	logger.Info("reconnecting with master")
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()
	mdb.replication.isMasterOnline = false
	mdb.replication.stopSlaveWithMutex()
	go mdb.syncWithMaster()
	return nil

}
func (mdb *MultiDB) startReplCron() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic", err)
			}
		}()
		ticker := time.Tick(time.Second)
		for range ticker {
			mdb.slaveCron()
		}
	}()
}

func (mdb *MultiDB) slaveCron() {
	repl := mdb.replication
	if repl.masterAckConn == nil {
		return
	}
	replTimeout := 60 * time.Second
	if config.Properties.ReplTimeout != 0 {
		replTimeout = time.Duration(config.Properties.ReplTimeout) * time.Second
	}

	minLastRecvTime := time.Now().Add(-replTimeout)
	// 如果上一次收到 master 的应答时间超过 60s，就认为主从已经断开连接了
	if repl.lastRecvTime.Before(minLastRecvTime) {
		logger.Info("receive master ack timeout: ", repl.lastRecvTime.Unix())
		err := mdb.reconnectWithMaster()
		if err != nil {
			logger.Error("send failed " + err.Error())
		}
		return
	}
	err := repl.sendAck2Master()
	if err != nil {
		logger.Error("send ack to master failed " + err.Error())
	}
	ackResp := <-repl.masterAckChan
	if protocol.IsOKReply(ackResp.Data) {
		repl.lastRecvTime = time.Now()
	}
}

// 这个是从节点执行的
func (mdb *MultiDB) execSlaveOf(c redis.Connection, args [][]byte) redis.Reply {
	// slaveof no one 断开所有连接
	for _, as := range args {
		logger.Info("args: ", string(as))
	}
	if strings.ToLower(string(args[0])) == "no" &&
		strings.ToLower(string(args[1])) == "one" {
		// 如果当前节点是 master 节点，且它收到了 slaveof no one slaveIp slavePort,
		// 那么它就应该吧这个 slave 从自己的 slaves 字典里剔除
		ss := fmt.Sprintf("ip: %s, port: %d", config.Properties.Bind, config.Properties.Port)
		logger.Info("ss: ", ss)
		for kk, _ := range mdb.slaves {
			logger.Info("mdb.slaves: ", kk)
		}
		if mdb.role == MasterRole && len(mdb.slaves) > 0 {
			if len(args) == 4 {
				key := fmt.Sprintf("%s:%s", string(args[2]), string(args[3]))
				logger.Info("master 执行 slaveof no one ", key )
				delete(mdb.slaves, key)
			} else {
				return protocol.MakeArgNumErrReply("slaveof no one slaveIp slavePort")
			}
		} else {
			mdb.slaveOfNone()
		}
		return protocol.MakeOkReply()
	}

	host := string(args[0])
	port, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR Port value is not an integer or out of range")
	}
	mdb.replication.mutex.Lock()
	// 修改节点的状态为 slave
	atomic.StoreInt32(&mdb.role, SlaveRole)

	mdb.replication.masterHost = host
	mdb.replication.masterPort = port

	atomic.AddInt32(&mdb.replication.modCount, 1)
	mdb.replication.mutex.Unlock()
	go mdb.syncWithMaster()
	return protocol.MakeOkReply()
}

func (mdb MultiDB) syncWithMaster() {
	//defer func() {
	//	if err := recover(); err != nil {
	//		logger.Error(err)
	//	}
	//}()

	mdb.replication.mutex.Lock()
	ctx, cancel := context.WithCancel(context.Background())

	mdb.replication.ctx = ctx
	mdb.replication.cancel = cancel
	mdb.replication.mutex.Unlock()
	/*
	1、和主服务器建立连接
	2、发送 ping 给主服务器
	3、身份验证
	4、向主服务器发送自己的 port
	 */
	logger.Info("连接 master ", mdb.replication.masterHost, mdb.replication.masterPort)
	err := mdb.connectWithMaster()
	if err != nil {
		logger.Info("连接 master error: ", err)
		return
	}

	// 开始同步
	err = mdb.doPsync()
	if err != nil {
		logger.Info("同步 master error: ", err)
		return
	}

	// 同步完成后，主从服务器之间开启 命令传播
	err = mdb.receiveAOF()
	if err != nil {
		logger.Info("命令传播 error: ", err)
		return
	}
}

func (mdb *MultiDB) connectWithMaster() error {
	modCount := atomic.LoadInt32(&mdb.replication.modCount)
	addr := mdb.replication.masterHost + ":" + strconv.Itoa(mdb.replication.masterPort)
	// 与 master 建立连接
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		mdb.slaveOfNone()
		return errors.New("connect master failed " + err.Error())
	}

	ackConn, ackErr := net.Dial("tcp", addr)
	if ackErr != nil {
		mdb.slaveOfNone()
		return errors.New("connect master failed " + ackErr.Error())
	}
	masterChan := parser.ParseStream(conn)
	ackChan := parser.ParseStream(ackConn)

	// slave 先给 master 发个 ping
	pingCmdLine := utils.ToCmdLine("ping")
	pingReq := protocol.MakeMultiBulkReply(pingCmdLine)
	_, err = conn.Write(pingReq.ToBytes())
	if err != nil {
		return errors.New("slave send ping failed " + err.Error())
	}
	pingResp := <-masterChan
	if pingResp.Err != nil {
		return errors.New("read ping response failed: " + pingResp.Err.Error())
	}

	switch reply := pingResp.Data.(type) {
	case *protocol.StandardErrReply:
		if !strings.HasPrefix(reply.Error(), "NOAUTH") &&
			!strings.HasPrefix(reply.Error(), "NOPERM") &&
			!strings.HasPrefix(reply.Error(), "ERR operation not permitted") {
			logger.Error("Error reply to PING from master: " + string(reply.ToBytes()))
			mdb.slaveOfNone()
			return nil
		}
	}

	sendCmdToMaster := func(conn net.Conn, cmdLine CmdLine, masterChan <-chan *parser.Payload) error {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err = conn.Write(req.ToBytes())
		if err != nil {
			mdb.slaveOfNone()
			return errors.New("send failed " + err.Error())
		}
		resp := <-masterChan
		if resp.Err != nil {
			mdb.slaveOfNone() // abort
			return errors.New("read response failed: " + resp.Err.Error())
		}
		if !protocol.IsOKReply(resp.Data) {
			mdb.slaveOfNone() // abort
			return errors.New("unexpected auth response: " + string(resp.Data.ToBytes()))
		}
		return nil
	}

	if config.Properties.MasterAuth != "" {
		authCmdLine := utils.ToCmdLine("auth", config.Properties.MasterAuth)
		err = sendCmdToMaster(conn, authCmdLine, masterChan)
		if err != nil {
			return err
		}
	}

	var port int
	if config.Properties.SlaveAnnouncePort != 0 {
		port = config.Properties.SlaveAnnouncePort
	} else {
		port = config.Properties.Port
	}

	portCmdLine := utils.ToCmdLine("REPLCONF", "listening-port", strconv.Itoa(port))
	err = sendCmdToMaster(conn, portCmdLine, masterChan)
	if err != nil {
		return err
	}

	// update connection
	mdb.replication.mutex.Lock()
	if mdb.replication.modCount != modCount {
		// replication conf changed during connecting and waiting mutex
		return nil
	}
	mdb.replication.masterConn = conn
	mdb.replication.masterChan = masterChan

	mdb.replication.masterAckConn = ackConn
	mdb.replication.masterAckChan = ackChan

	// 最近一次收到 master 回复的时间
	mdb.replication.lastRecvTime = time.Now()
	mdb.replication.isMasterOnline = true
	mdb.replication.mutex.Unlock()
	return nil
}

func (mdb *MultiDB) doPsync() error {
	// 请求主服务器进行完整重同步
	/*
	这里原作者没有判断到底是进行完整重同步还是部分重同步
	直接默认使用完整重同步
	 */
	// 根据 slave 的复制偏移量来判断是做完整重同步，还是部分重同步
	if mdb.replication.replOffset > 0 && mdb.replication.replId != "" {
		// slave 发出：需要部分重同步
		logger.Info("slave 发起部分重同步")
		psyncCmd := utils.ToCmdLine("psync", mdb.replication.replId, strconv.FormatInt(mdb.replication.replOffset, 10))
		logger.Info("psync ", mdb.replication.replId, " ", strconv.FormatInt(mdb.replication.replOffset, 10))
		opCode, statusReply, opErr := mdb.sendAndJudgePsync(psyncCmd)
		// 根据 master 的返回值判断是否做完整重同步
		if opErr != nil || opCode == 0 {
			return mdb.doFullSync(statusReply)
		} else {
			return mdb.doPartSync()
		}
	} else {
		// 完整重同步
		logger.Info("slave 发起完整重同步")
		psyncCmdLine := utils.ToCmdLine("psync", "?", "-1")
		_, statusReply, err := mdb.sendAndJudgePsync(psyncCmdLine)
		if err != nil {
			return err
		}
		// master 在响应完整重同步后，会发自己的 replId 给 slave
		ls := strings.Split(statusReply.Status, " ")
		mdb.replication.replId = ls[1]
		return mdb.doFullSync(statusReply)
	}
}

func (mdb *MultiDB) doPartSync() error {
	// 开始接受来自 master 的写命令，这个命令传播很相似
	/* 这里其实什么也不用做，因为当 slave 和 master 不同步时，
	要么做完整重同步，要么做部分重同步
	如果要做部分重同步时，因为主从建立连接后，会有 心跳检测，
	心跳检测时，如果 master 发现 slave 不同步，会立刻开始同步命令给 slave，
	不需要 slave 再额外发起请求
	所以这里什么也不做
	 */
	return nil

}

func (mdb *MultiDB) doFullSync(reply *protocol.StatusReply) error {
	// 完整重同步
	modCount := atomic.LoadInt32(&mdb.replication.modCount)
	headers := strings.Split(reply.Status, " ")
	ch := mdb.replication.masterChan
	psyncPayload2 := <-ch
	if psyncPayload2.Err != nil {
		return errors.New("read response failed: " + psyncPayload2.Err.Error())
	}
	// 从主服务器获取 rdb 文件
	// 主服务器没有生成 rdb 文件，没有数据要同步
	if reflect.TypeOf(psyncPayload2.Data).String() == reflect.TypeOf(&protocol.NullBulkReply{}).String() {
		return nil
	}
	psyncData, ok := psyncPayload2.Data.(*protocol.RDBFileReply)
	if !ok {
		return errors.New("illegal payload header: " + string(psyncPayload2.Data.ToBytes()))
	}

	logger.Info(fmt.Sprintf("receive %d bytes of rdb from master", len(psyncData.Arg)))

	rdbBytes, rdbErr := psyncData.ToRDBFileBytes()
	if rdbErr != nil {
		return errors.New("receive rdb file from master error: " +  rdbErr.Error())
	}
	rdbDec := rdb.NewDecoder(bytes.NewReader(rdbBytes))
	rdbHolder := MakeBasicMultiDB()
	err := dumpRDB(rdbDec, rdbHolder)
	if err != nil {
		return errors.New("dump rdb failed: " + err.Error())
	}
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()

	// todo 这里不太明白
	if mdb.replication.modCount != modCount {
		// replication conf changed during connecting and waiting mutex
		return nil
	}

	// 这里存的是 master 的运行 id
	mdb.replication.replId = headers[1]
	mdb.replication.replOffset, err = strconv.ParseInt(headers[2], 10, 64)
	if err != nil {
		return errors.New("get illegal repl offset: " + headers[2])
	}

	//logger.Info("full resync from master: " + mdb.replication.replId)
	//logger.Info("current offset:", mdb.replication.replOffset)

	for i, h := range rdbHolder.dbSet {
		newDB := h.Load().(*DB)
		mdb.loadDB(i, newDB)
	}

	return nil
}
/*
向 master 发送同步命令，并通过 master 的返回值，判断是做完整重同步还是部分重同步
0 and error == nil or error 不为空 -> 完整重同步
1 and error == nil -> 部分重同步
*/
func (mdb *MultiDB) sendAndJudgePsync(psyncCmd [][]byte) (int, *protocol.StatusReply, error) {
	psyncReq := protocol.MakeMultiBulkReply(psyncCmd)
	_, err := mdb.replication.masterConn.Write(psyncReq.ToBytes())
	if err != nil {
		logger.Error("mdb.replication.masterConn.Write error", err.Error())
		return 0, nil, errors.New("send failed " + err.Error())
	}
	// 仍要根据 master 的返回值确定做哪种类型的同步
	ch := mdb.replication.masterChan
	psyncPayload1 := <-ch
	if psyncPayload1.Err != nil {
		logger.Error(errors.New("psync response failed: " + psyncPayload1.Err.Error()))
		return 0, nil, errors.New("psync response failed: " + psyncPayload1.Err.Error())
	}
	psyncHeader, ok := psyncPayload1.Data.(*protocol.StatusReply)
	if !ok {
		logger.Error(errors.New("illegal payload header: " + string(psyncPayload1.Data.ToBytes())))
		return 0, nil, errors.New("illegal payload header: " + string(psyncPayload1.Data.ToBytes()))
	}
	logger.Info("status: ", psyncHeader.Status)
	if strings.HasPrefix(psyncHeader.Status, "fullsync") {
		return 0, psyncHeader, nil
	} else if strings.HasPrefix(psyncHeader.Status, "continue") {
		return 1, psyncHeader, nil
	}

	return 0, nil, errors.New(psyncHeader.Status)
}

// 主从模式，命令传播，slave 从 master 那获取写命令，然后在本地执行
func (mdb *MultiDB) receiveAOF() error {
	//logger.Info("receiveAOF, addr: ", mdb.replication.masterConn.RemoteAddr())
	conn := connection.NewConn(mdb.replication.masterConn)
	conn.SetRole(connection.ReplicationRecvCli)
	mdb.replication.mutex.Lock()
	modCount := mdb.replication.modCount
	mdb.replication.isRDBSyncDone = true
	done := mdb.replication.ctx.Done()
	mdb.replication.mutex.Unlock()
	if done == nil {
		return nil
	}

	mdb.replication.running.Add(1)
	defer mdb.replication.running.Done()
	for {
		select {
		case payload := <-mdb.replication.masterChan:
			if payload.Err != nil {
				//logger.Info("receiveAOF, payload error, ", payload.Err)
				return payload.Err
			}

			if protocol.IsOKReply(payload.Data) {
				logger.Info("receiveAOF receive OK Reply")
				mdb.replication.lastRecvTime = time.Now()
				break
			}

			if reflect.TypeOf(payload.Data).String() == reflect.TypeOf(&protocol.StatusReply{}).String() {
				statusReply := payload.Data.(*protocol.StatusReply)
				logger.Info("receiveAOF receive status reply: ", statusReply.Status)
				break
			}

			cmdLine, bulkOk := payload.Data.(*protocol.MultiBulkReply)
			if !bulkOk {
				//logger.Info("unexpected payload: " + string(payload.Data.ToBytes()))
				return errors.New("unexpected payload: " + string(payload.Data.ToBytes()))
			}
			//logger.Info("receiveAOF payload: ", string(cmdLine.Args[0]))
			mdb.replication.mutex.Lock()
			if mdb.replication.modCount != modCount {
				return nil
			}
			mdb.Exec(conn, cmdLine.Args)
			// todo: directly get size from socket
			//n := len(cmdLine.ToBytes())
			mdb.replication.replOffset++
			//logger.Info(fmt.Sprintf("receive %d bytes from master, current offset %d",
			//	n, mdb.replication.replOffset))
			mdb.replication.mutex.Unlock()
		case <-done:
			//logger.Info("receiveAOF done")
			return nil
		}
	}
}

func (mdb *MultiDB) slaveOfNone() {
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()

	// 当前节点执行 slaveof no one 时，如果它原本是 slave 节点，那么它需要告诉 master 节点，让 master 把它从 slaves 里删掉
	cmdLine := utils.ToCmdLine("slaveof", "no", "one", config.Properties.Bind, strconv.Itoa(config.Properties.Port))
	if mdb.replication.masterConn != nil {
		_, err := mdb.replication.masterConn.Write(protocol.MakeMultiBulkReply(cmdLine).ToBytes())
		// 这里有种特殊情况，在 sentinel 模式下，如果之前的 master 挂了，那么这条命令是无法执行的
		if err != nil {
			logger.Error("error on execute slaveof no one in slave node. Old master might be down")
		}
	}

	mdb.replication.masterHost = ""
	mdb.replication.masterPort = 0
	mdb.replication.replId = ""
	mdb.replication.replOffset = 0
	atomic.StoreInt32(&mdb.role, MasterRole)
	mdb.replication.stopSlaveWithMutex()
}

// stopSlaveWithMutex stops in-progress connectWithMaster/fullSync/receiveAOF
// invoker should have replication mutex
func (repl *replicationStatus) stopSlaveWithMutex() {
	// update modCount to stop connectWithMaster and fullSync
	atomic.AddInt32(&repl.modCount, 1)
	// send cancel to receiveAOF
	if repl.cancel != nil {
		/*
		这个 cancel 函数来自于 withCancel，当这个 cancel 函数被调用时，receiveAOF 里的 done 通道就会被关闭，
		而通道关闭，将导致退出 select 可以参考：https://www.jianshu.com/p/d68715c5b341
		ctx, cancel := context.WithCancel(context.Background())
		mdb.replication.ctx = ctx
		mdb.replication.cancel = cancel
		 */
		repl.cancel()
		repl.running.Wait()
	}
	repl.ctx = context.Background()
	repl.cancel = nil
	if repl.masterConn != nil {
		_ = repl.masterConn.Close() // parser.ParseStream will close masterChan
	}

	if repl.masterAckConn != nil {
		_ = repl.masterAckConn.Close()
	}

	repl.masterConn = nil
	repl.masterChan = nil

	repl.masterAckConn = nil
	repl.masterAckChan = nil

}

func (mdb *MultiDB) WriteReplOffsetToFile() {
	fobj, err := os.OpenFile(mdb.replication.replOffsetFileName, os.O_CREATE | os.O_RDWR, 0744)
	if err != nil{
		logger.Error(fmt.Sprintf("打开 repl_offset 文件失败,错误为:%v\n",err))
		return
	}

	defer fobj.Close()

	writer := bufio.NewWriter(fobj) //往文件里面写入内容，得到了一个writer对象
	_, _ = writer.WriteString(fmt.Sprintf("repl_offset %d\n", mdb.replication.replOffset))
	logger.Info("+++++++++ ", mdb.replication.replId)
	_, _ = writer.WriteString(fmt.Sprintf("repl_id %s\n", mdb.replication.replId))
	_ = writer.Flush()
}

func readReplConf(repl *replicationStatus) {
	if utils.PathExists(repl.replOffsetFileName) {
		fobj, err := os.OpenFile(repl.replOffsetFileName, os.O_RDONLY, 0744)
		if err != nil{
			logger.Error(fmt.Sprintf("打开 repl_offset 文件失败,错误为:%v\n",err))
			return
		}

		defer fobj.Close()

		//使用buffio读取文件内容
		reader := bufio.NewReader(fobj) //创建新的读的对象
		for {
			line , err := reader.ReadString('\n') //注意是字符，换行符。
			if err == io.EOF{
				logger.Info("文件读完了")
				break
			}
			if err != nil{ //错误处理
				logger.Error("读取文件失败,错误为: ", err)
				return
			}
			line = strings.TrimSpace(line)
			ls := strings.Split(line, " ")
			if len(ls) != 2 {
				continue
			}
			if ls[0] == "repl_offset" {
				repl.replOffset, err = strconv.ParseInt(ls[1], 10, 64)
			} else if ls[0] == "repl_id" {
				repl.replId = ls[1]
			}
		}
	}

}