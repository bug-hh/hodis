package database

import (
	"fmt"
	"github.com/hodis/aof"
	"github.com/hodis/config"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/pubsub"
	"github.com/hodis/redis/connection"
	"github.com/hodis/redis/protocol"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// MultiDB is a set of multiple database set
type MultiDB struct {
	dbSet []*atomic.Value

	// todo 需要时再实现 handle publish/subscribe
	hub *pubsub.Hub

	aofHandler *aof.Handler

	// store master node address
	slaveOf     string
	role int32
	// todo 需要时再实现
	replication *replicationStatus

	// 如果节点本身是 master 节点的话，这里存储所有 slave 节点的地址
	// 存储的目的是为了在 info replication 命令时，显示 slave 信息
	slaves map[string]redis.Connection

	monitors []redis.Connection
}

func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{
		slaves: make(map[string]redis.Connection),
	}
	if config.Properties.Databases == 0 {
		// 默认 16 个数据库
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := MakeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		mdb.dbSet[i] = holder
	}

	// todo 以后实现订阅，发布功能
	mdb.hub = pubsub.MakeHub()

	// aof 功能
	validAof := false
	if config.Properties.AppendOnly {
		/*
		在 NewAOFHandler 里，
		先读取已有的 aof 文件，然后开启子协程，从 channel 里读主协程写入的命令，
		然后往 aof 文件里写入读到的命令
		 */
		aofHandler, err := aof.NewAOFHandler(mdb, func() database.EmbedDB {
			return MakeBasicMultiDB()
		})

		if err != nil {
			panic(err)
		}

		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			singleDB := db.Load().(*DB)
			singleDB.addAof = func(line CmdLine) {
				// 主协程往 aofChan 里写命令
				mdb.aofHandler.AddAof(singleDB.index, line)
			}
		}
		validAof = true
	}

	// todo 以后自己 rdb 解析
	// 按照 redis 的实现，如果开启了 aof，就不会使用 rdb
	if config.Properties.RDBFilename != "" && !validAof {
		loadRdbFile(mdb)
	}

	// 主从模式复制
	mdb.replication = initReplStatus()
	mdb.startReplCron()
	// 初始化时，默认自己是 master
	mdb.role = MasterRole

	return mdb
}

func MakeBasicMultiDB() *MultiDB {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		holder := &atomic.Value{}
		holder.Store(makeBasicDB())
		mdb.dbSet[i] = holder
	}
	return mdb
}
// 实现 EmbedDB 接口里的所有方法
func (mdb *MultiDB) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	// 先从客户端连接从获取数据库编号
	db, errReply := mdb.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}

	return db.execWithLock(cmdLine)
}

func (mdb *MultiDB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	selectedDB, errReply := mdb.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectedDB.ExecMulti(conn, watching, cmdLines)
}

func (mdb *MultiDB) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return mdb.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

func (mdb *MultiDB) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	mdb.mustSelectDB(dbIndex).ForEach(cb)
}

func (mdb *MultiDB) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	mdb.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

func (mdb *MultiDB) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	mdb.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

func (mdb *MultiDB) GetDBSize(dbIndex int) (int, int) {
	db := mdb.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

// 实现 DB 接口里的所有方法，从而实现 redis 风格的数据库存储引擎
func (mdb *MultiDB) Exec(c redis.Connection, cmdLine [][]byte)  (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName != "ping" && cmdName != "replconf" {
		logger.Info("MultiDB cmdName: ", cmdName)
	}

	if cmdName == "monitor" {
		// 服务器收到这个消息后，把对应 client 连接加入 monitors 链表
		mdb.monitors = append(mdb.monitors, c)
		return protocol.MakeOkReply()
	}

	// todo 之后实现 authenticate
	if cmdName == "auth" {

	}
	// 处理来自 slave 节点发送的消息
	if cmdName == "replconf" {
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("replconf")
		}
		if mdb.role == SlaveRole {
			return protocol.MakeErrReply("slave node cannot handle replconf command")
		}
		key := strings.ToLower(string(cmdLine[1]))
		if key == "listening-port" {
			// 在收到来自 slave 的 ack 后，初始化用于命令传播的回调函数
			for _, db := range mdb.dbSet {
				singleDB := db.Load().(*DB)
				// 用于主从模式下的命令传播
				if singleDB.cmdSync != nil {
					continue
				}
				singleDB.cmdSync = func(line CmdLine) (ret error) {
					logger.Info("xxx sync")
					// 只有自己是 master，才有资格向 slave 传播写命令
					if atomic.LoadInt32(&mdb.role) == MasterRole {
						keyCommand := string(line[0])
						logger.Info("command sync key: ", keyCommand)
						// todo 这里应该循环遍历所有的 salve connection
						for _, cc := range mdb.slaves {
							syncReq := protocol.MakeMultiBulkReply(line)
							ret = cc.Write(syncReq.ToBytes())
							if ret != nil {
								logger.Info("command sync failed: ", ret.Error())
							}
						}
						// 记录
						mdb.replication.mutex.Lock()
						defer mdb.replication.mutex.Unlock()
						mdb.replication.replBuffer[mdb.replication.replOffset] = line
						mdb.replication.replOffset = (mdb.replication.replOffset + 1) % REPL_BUFFER_SIZE
					}
					return ret
				}
			}
			cc := c.(*connection.Connection)
			mdb.slaves[cc.RemoteAddr().String()] = c
			return protocol.MakeOkReply()
		} else if key == "ack" {
			// 收到来自 slave 的 ack
			slaveReplOffset, err := strconv.ParseInt(string(cmdLine[2]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("illegal slave replication offset")
			}
			// master 需要处理来自 slave 的 offset，如果 offset 与 master 记录的 offset 有偏差，证明主从不一致，需要同步命令
			if slaveReplOffset < mdb.replication.replOffset {
				go sendCmdToSlave(slaveReplOffset, mdb, c)
			}
			return protocol.MakeOkReply()
		} else {
			return protocol.MakeErrReply("unknown command for replconf")
		}
	} else if cmdName == "psync" {
		// 区分是完整重同步，还是部分重同步
		// 完整重同步
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("psync")
		}
		if mdb.role == SlaveRole {
			return protocol.MakeErrReply("slave node cannot handle psync command")
		}
		// 收到来自 slave 节点的完整重同步命令
		if string(cmdLine[1]) == "?" && string(cmdLine[2]) == "-1" {
			go SaveAndSendRDB(mdb, c)
			// 向 slave 节点发送 +fullsync runId offset 的回复
			// 这里我们直接用 master 的 ip:port 地址作为 runId 来回复，这时的 offset 为 0
			msg := fmt.Sprintf("fullsync %s:%d 0", config.Properties.Bind, config.Properties.Port)
			return protocol.MakeStatusReply(msg)
		} else {
			// 收到来自 slave 节点的部分重同步命令
			// 先判断 runid 是否匹配，如果不匹配，则回复 slave 进行完整重同步，我们这里用的 runid 就是 masterip:masterport
			runId := string(cmdLine[1])
			offset, err := strconv.ParseInt(string(cmdLine[2]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("illegal slave replication offset")
			}
			// 进行完整重同步
			if runId != fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port) {
				msg := fmt.Sprintf("fullsync %s:%d 0", config.Properties.Bind, config.Properties.Port)
				go SaveAndSendRDB(mdb, c)
				return protocol.MakeStatusReply(msg)
			}
			// 进行部分重同步
			for i:=offset;i<mdb.replication.replOffset;i++ {
				syncReq := protocol.MakeMultiBulkReply(mdb.replication.replBuffer[i])
				ret := c.Write(syncReq.ToBytes())
				if ret != nil {
					logger.Info("command sync failed: ", ret.Error())
				}
			}
		}
	}

	// slaveof ip port, 当前节点进入 slave 模式，slave 模式下，不容许 slave 节点执行来自客户端的 set 操作
	if cmdName == "slaveof" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot use slave of database within multi")
		}
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("SLAVEOF")
		}
		return mdb.execSlaveOf(c, cmdLine[1:])
	}

	// todo 特殊命令，以后实现
	// subscribe, publish, save, copy
	if cmdName == "bgrewriteaof" {
		return BGRewriteAOF(mdb, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		return RewriteAOF(mdb, cmdLine[1:])
	} else if cmdName == "bgsave" {
		return BGSaveRDB(mdb)
	} else if cmdName == "subscribe" {
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("subscribe")
		}
		return pubsub.Subscribe(mdb.hub, c, cmdLine[1:])
	} else if cmdName == "publish" {
		return pubsub.Publish(mdb.hub, cmdLine[1:])
	}
	// 如果本节点时 slave 节点，那么不允许 slave 接受来自客户端的写操作（擅自进行写操作，应该由 master 节点进行同步）
	// 这里统一拦截掉
	cc := c.(*connection.Connection)
	if !CanExecWriteCmd(mdb, cc, cmdName) {
		return protocol.MakeErrReply("READONLY You can't write against a read only replica.")
	}

	// 处理来自 sentinel 服务器的 info 命令
	if cmdName == "info" {
		//return protocol.MakeStatusReply("receive info from sentinel")
		// 只返回给 sentinel 主从配置信息
		return mdb.handleInfo()
	}
	// 常规命令
	dbIndex := c.GetDBIndex()
	selectDB, errReply := mdb.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}

	if len(mdb.monitors) > 0 {
		sendCommandToMonitor(mdb, c, cmdLine)
	}

	return selectDB.Exec(c, cmdLine)
}

func sendCommandToMonitor(mdb *MultiDB, conn redis.Connection, cmdLine CmdLine) {
	c := conn.(*connection.Connection)
	utils.ToCmdLine()
	msg := fmt.Sprintf("%+v [%d %s] %s", time.Now().Unix(), conn.GetDBIndex(), c.RemoteAddr().String(), utils.FormatCmdLine(cmdLine))
	time.Now().Unix()
	for _, m := range mdb.monitors {
		err := m.Write(protocol.MakeStatusReply(msg).ToBytes())
		if err != nil {
			logger.Info("send to monitor failed")
		}
	}
}

func sendCmdToSlave(slaveReplOffset int64, mdb *MultiDB, c redis.Connection) {
	for i:=slaveReplOffset;i<mdb.replication.replOffset;i++ {
		syncReq := protocol.MakeMultiBulkReply(mdb.replication.replBuffer[i])
		ret := c.Write(syncReq.ToBytes())
		if ret != nil {
			logger.Info("command sync failed: ", ret.Error())
		}
	}
}

func CanExecWriteCmd(db *MultiDB, conn *connection.Connection, cmdName string) bool {
	writeCmds := []string{
		"set", "mset", "lpush", "rpush", "lpop", "rpop", "zadd", "zrem",
	}
	for _, cmd := range writeCmds {
		return !(cmdName == cmd && atomic.LoadInt32(&db.role) == SlaveRole && conn.GetRole() != connection.ReplicationRecvCli)
	}
	return true
}
func SaveAndSendRDB(db *MultiDB, conn redis.Connection) {
	BGSaveRDB(db)
	err := sendRDBFile(conn)
	if err != nil {
		logger.Info("send rdb file error: ", err.Error())
	}
}

func BGSaveRDB(db *MultiDB)	redis.Reply {
	if db.aofHandler == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(err)
			}
		}()
		err := db.aofHandler.Rewrite2RDB()
		if err != nil {
			logger.Error(err)
		}
	}()
	return protocol.MakeStatusReply("Background saving started")
}

func (mdb *MultiDB) selectDB(dbIndex int) (*DB, *protocol.StandardErrReply) {
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR DB Index is out of range")
	}

	return mdb.dbSet[dbIndex].Load().(*DB), nil
}

func (mdb *MultiDB) mustSelectDB(dbIndex int) *DB {
	selectDB, errReply := mdb.selectDB(dbIndex)
	if errReply != nil {
		panic(errReply)
	}
	return selectDB
}


// AfterClientClose does some clean after client close connection
func (mdb *MultiDB) AfterClientClose(c redis.Connection) {

}

// Close graceful shutdown database
func (mdb *MultiDB) Close() {
	// stop replication first

}


func BGRewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	go db.aofHandler.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// RewriteAOF start Append-Only-File rewriting and blocked until it finished
func RewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	err := db.aofHandler.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

func (mdb *MultiDB) loadDB(dbIndex int, newDB *DB) redis.Reply {
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	oldDB := mdb.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof // inherit oldDB
	mdb.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
}

func (mdb *MultiDB) calcSlaves() []string {
	var ret []string
	i := 0
	for _, conn := range mdb.slaves {
		c := conn.(*connection.Connection)
		addrs := strings.Split(c.RemoteAddr().String(), ":")
		slaveInfo := fmt.Sprintf("slave%d:ip=%s,port=%s,state=online,offset=%d,lag=%d",
			i, addrs[0], addrs[1], mdb.replication.replOffset, time.Now().Unix()-mdb.replication.lastRecvTime.Unix())
		ret = append(ret, slaveInfo)
		i++
	}
	return ret
}

func (mdb *MultiDB) handleInfo() redis.Reply {
	text := []string{
		"# Replication",
		"role:master",
		fmt.Sprintf("connected_slaves:%d", len(mdb.slaves)),
	}
	text = append(text, mdb.calcSlaves()...)
	text = append(text, fmt.Sprintf("master_replid:%s:%d", config.Properties.Bind, config.Properties.Port))
	text = append(text, fmt.Sprintf("master_repl_offset:%d", mdb.replication.replOffset))

	var content [][]byte
	for _, s := range text {
		bs := []byte(s)
		content = append(content, bs)
	}
	return protocol.MakeMultiBulkReply(content)
}