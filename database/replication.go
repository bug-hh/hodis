package database

import (
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
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	masterRole = iota
	slaveRole
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
	lastRecvTime time.Time
	running sync.WaitGroup

	// 复制缓冲区
	replBuffer [][][]byte
}

func initReplStatus() *replicationStatus {
	repl := &replicationStatus{
		replBuffer: make([][][]byte, REPL_BUFFER_SIZE),
	}
	return repl
}

func (repl *replicationStatus) sendAck2Master() error {
	psyncCmdLine := utils.ToCmdLine("REPLCONF", "ACK",
		strconv.FormatInt(repl.replOffset, 10))
	psyncReq := protocol.MakeMultiBulkReply(psyncCmdLine)
	_, err := repl.masterConn.Write(psyncReq.ToBytes())
	//_, err := repl.masterAckConn.Write(psyncReq.ToBytes())
	return err
}

func (mdb *MultiDB) reconnectWithMaster() error {
	logger.Info("reconnecting with master")
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()
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
	if repl.masterConn == nil {
		return
	}
	replTimeout := 60 * time.Second
	if config.Properties.ReplTimeout != 0 {
		replTimeout = time.Duration(config.Properties.ReplTimeout) * time.Second
	}

	minLastRecvTime := time.Now().Add(-replTimeout)
	// 如果上一次收到 master 的应答时间超过 60s，就认为主从已经断开连接了
	if repl.lastRecvTime.Before(minLastRecvTime) {
		logger.Info("slave timeout: ", repl.lastRecvTime.Unix())
		err := mdb.reconnectWithMaster()
		if err != nil {
			logger.Error("send failed " + err.Error())
		}
		return
	}
	err := repl.sendAck2Master()
	if err != nil {
		logger.Error("send failed " + err.Error())
	}
	//ackResp := <-repl.masterAckChan
	//if protocol.IsOKReply(ackResp.Data) {
	//	repl.lastRecvTime = time.Now()
	//}
}

func (mdb *MultiDB) execSlaveOf(c redis.Connection, args [][]byte) redis.Reply {
	// slaveof no one 断开所有连接
	if strings.ToLower(string(args[0])) == "no" &&
		strings.ToLower(string(args[1])) == "one" {
		mdb.slaveOfNone()
		return protocol.MakeOkReply()
	}

	host := string(args[0])
	port, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR Port value is not an integer or out of range")
	}
	mdb.replication.mutex.Lock()
	// 修改节点的状态为 slave
	atomic.StoreInt32(&mdb.role, slaveRole)

	mdb.replication.masterHost = host
	mdb.replication.masterPort = port

	atomic.AddInt32(&mdb.replication.modCount, 1)
	mdb.replication.mutex.Unlock()
	go mdb.syncWithMaster()
	return protocol.MakeOkReply()
}

func (mdb MultiDB) syncWithMaster() {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

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
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		mdb.slaveOfNone()
		return errors.New("connect master failed " + err.Error())
	}

	//ackConn, ackErr := net.Dial("tcp", addr)
	//if ackErr != nil {
	//	mdb.slaveOfNone()
	//	return errors.New("Ack channel to master disconnected: " + ackErr.Error())
	//}

	masterChan := parser.ParseStream(conn)
	//ackChan := parser.ParseStream(ackConn)

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

	// 在 redis 实现里，没有这个
	// announce ip
	//if config.Properties.SlaveAnnounceIP != "" {
	//	ipCmdLine := utils.ToCmdLine("REPLCONF", "ip-address", config.Properties.SlaveAnnounceIP)
	//	err = sendCmdToMaster(conn, ipCmdLine, masterChan)
	//	if err != nil {
	//		return err
	//	}
	//}

	// 在 redis 实现里，没有这个
	// announce capacity
	//capaCmdLine := utils.ToCmdLine("REPLCONF", "capa", "psync2")
	//err = sendCmdToMaster(conn, capaCmdLine, masterChan)
	//if err != nil {
	//	return err
	//}

	// update connection
	mdb.replication.mutex.Lock()
	if mdb.replication.modCount != modCount {
		// replication conf changed during connecting and waiting mutex
		return nil
	}
	mdb.replication.masterConn = conn
	mdb.replication.masterChan = masterChan

	//mdb.replication.masterAckConn = ackConn
	//mdb.replication.masterAckChan = ackChan

	// 最近一次收到 master 回复的时间
	mdb.replication.lastRecvTime = time.Now()
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
		psyncCmd := utils.ToCmdLine("psync", mdb.replication.replId, strconv.FormatInt(mdb.replication.replOffset, 64))
		opCode, statusReply, opErr := mdb.sendAndJudgePsync(psyncCmd)
		// 根据 master 的返回值判断是否做完整重同步
		if opErr != nil || opCode == 0 {
			return mdb.doFullSync(statusReply)
		} else {
			return mdb.doPartSync()
		}
	} else {
		// 完整重同步
		psyncCmdLine := utils.ToCmdLine("psync", "?", "-1")
		_, statusReply, err := mdb.sendAndJudgePsync(psyncCmdLine)
		if err != nil {
			return err
		}
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

	mdb.replication.replId = headers[1]
	mdb.replication.replOffset, err = strconv.ParseInt(headers[2], 10, 64)
	if err != nil {
		return errors.New("get illegal repl offset: " + headers[2])
	}

	logger.Info("full resync from master: " + mdb.replication.replId)
	logger.Info("current offset:", mdb.replication.replOffset)

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
		return 0, nil, errors.New("send failed " + err.Error())
	}
	// 仍要根据 master 的返回值确定做哪种类型的同步
	ch := mdb.replication.masterChan
	psyncPayload1 := <-ch
	if psyncPayload1.Err != nil {
		return 0, nil, errors.New("psync response failed: " + psyncPayload1.Err.Error())
	}
	psyncHeader, ok := psyncPayload1.Data.(*protocol.StatusReply)
	if !ok {
		return 0, nil, errors.New("illegal payload header: " + string(psyncPayload1.Data.ToBytes()))
	}
	if strings.HasPrefix(psyncHeader.Status, "fullsync") {
		return 0, psyncHeader, nil
	} else if strings.HasPrefix(psyncHeader.Status, "continue") {
		return 1, psyncHeader, nil
	}

	return 0, nil, errors.New(psyncHeader.Status)
}

// 主从模式，命令传播，slave 从 master 那获取写命令，然后在本地执行
func (mdb *MultiDB) receiveAOF() error {
	logger.Info("receiveAOF, addr: ", mdb.replication.masterConn.RemoteAddr())
	conn := connection.NewConn(mdb.replication.masterConn)
	conn.SetRole(connection.ReplicationRecvCli)
	mdb.replication.mutex.Lock()
	modCount := mdb.replication.modCount
	done := mdb.replication.ctx.Done()
	mdb.replication.mutex.Unlock()
	if done == nil {
		return nil
	}

	mdb.replication.running.Add(1)
	defer mdb.replication.running.Done()
	logger.Info("enter for")
	for {
		select {
		case payload := <-mdb.replication.masterChan:
			if payload.Err != nil {
				logger.Info("receiveAOF, payload error, ", payload.Err)
				return payload.Err
			}

			if protocol.IsOKReply(payload.Data) {
				mdb.replication.lastRecvTime = time.Now()
				break
			}

			cmdLine, bulkOk := payload.Data.(*protocol.MultiBulkReply)
			if !bulkOk {
				logger.Info("unexpected payload: " + string(payload.Data.ToBytes()))
				return errors.New("unexpected payload: " + string(payload.Data.ToBytes()))
			}
			logger.Info("receiveAOF payload: ", string(cmdLine.Args[0]))
			mdb.replication.mutex.Lock()
			if mdb.replication.modCount != modCount {
				return nil
			}
			mdb.Exec(conn, cmdLine.Args)
			// todo: directly get size from socket
			n := len(cmdLine.ToBytes())
			mdb.replication.replOffset++
			logger.Info(fmt.Sprintf("receive %d bytes from master, current offset %d",
				n, mdb.replication.replOffset))
			mdb.replication.mutex.Unlock()
		case <-done:
			logger.Info("receiveAOF done")
			return nil
		}
	}
}

func (mdb *MultiDB) slaveOfNone() {
	mdb.replication.mutex.Lock()
	defer mdb.replication.mutex.Unlock()

	mdb.replication.masterHost = ""
	mdb.replication.masterPort = 0
	mdb.replication.replId = ""
	mdb.replication.replOffset = -1
	mdb.replication.stopSlaveWithMutex()
}

// stopSlaveWithMutex stops in-progress connectWithMaster/fullSync/receiveAOF
// invoker should have replication mutex
func (repl *replicationStatus) stopSlaveWithMutex() {
	// update modCount to stop connectWithMaster and fullSync
	atomic.AddInt32(&repl.modCount, 1)
	// send cancel to receiveAOF
	if repl.cancel != nil {
		repl.cancel()
		repl.running.Wait()
	}
	repl.ctx = context.Background()
	repl.cancel = nil
	if repl.masterConn != nil {
		_ = repl.masterConn.Close() // parser.ParseStream will close masterChan
	}
	repl.masterConn = nil
	repl.masterChan = nil

	repl.masterAckConn = nil
	repl.masterAckChan = nil
}

