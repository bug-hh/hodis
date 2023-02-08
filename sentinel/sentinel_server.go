package sentinel

import (
	"context"
	"fmt"
	"github.com/hodis/config"
	"github.com/hodis/database"
	"github.com/hodis/datastruct/dict"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/idgenerator"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/connection"
	"github.com/hodis/redis/parser"
	"github.com/hodis/redis/protocol"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

//sentinel 配置文件解析：https://www.jianshu.com/p/d237c7210dbd

const (
	DICT_SIZE = 1 << 16
	SMALL_DICT_SIZE = 1 << 3
	QUORUM = "quorum"
	DOWN_AFTER_PERIOD = "down_after_milliseconds"
	MASTER_NAME = "master_name"
	IP = "ip"
	PORT = "port"
	PARALLEL_SYNCS = "parallel_syncs"
	SENTINEL_CHANNEL = "_sentinel_:hello"
	SUB_CMD = "subscribe"
)

const (
	// 正常在线
	ONLINE = iota
	// 主观下线
	SRI_S_DOWN
	// 客观下线
	SRI_O_DOWN
)


const (
	// 领导者
	LEADER = iota
	// 候选者
	CANDIDATE
	// 追随者
	FOLLOWER
)


type SentinelState struct {
	Masters dict.Dict
}

type SentinelRedisInstance struct {
	// 表示该节点是 master 还是 slave
	Role int

	// 代表节点状态，主观下线，客观下线，正常在线
	Flag int

	// 节点名字, 一般是 ip:port
	Name string

	RunID string

	//实例无响应多少毫秒后，才会被判断为主观下线
	DownAfterPeriod int

	// 判断这个实例客观下线所需的投票数
	// sentinel monitor <master-name> <ip> <redis-port> <quorum>
	Quorum int

	Addr *SentinelAddr

	Slaves dict.Dict

	ParallelSyncs int
}

// 保存被 sentinel 监控的实例的 ip 和 port
type SentinelAddr struct {
	Ip string
	Port int
}

type SentinelServer struct {
	state *SentinelState
	// 这里 masters 里 key 是 配置文件里的 master name
	// 而如果是 slave，key 则是 ip:port
	// 添加对 master, slave 的 connection
	// 连接有两种，一种是命令连接，一种是订阅连接
	cmdConnMap map[string]redis.Connection
	subConnMap map[string]redis.Connection

	// 对应以上两个连接的回复 channel
	cmdChanMap map[string]<-chan *parser.Payload
	subChanMap map[string]<-chan *parser.Payload

	//保存与其他 sentinel 之间的 连接
	selConnMap map[string]redis.Connection
	selChanMap map[string]<-chan *parser.Payload

	// 用于记录和 sentinel 连接出现异常的节点的异常起始时间
	// port:ip -> 异常开始时间
	pingMap map[string]int64

	// 用于记录「认为指定 node 节点为主观下线状态的 sentinel 节点数」
	countMap map[string]int

	ctx context.Context
	cancel context.CancelFunc

	RunID string

	// 配置纪元，等价于 raft 算法中的任期
	currentEpoch int

	// 这里存放所有其他的 sentinel 实例
	// ip:port -> SentinelRedisInstance
	Sentinels dict.Dict

	RaftRole int

	// ip:port
	RaftLeader string

	mu sync.RWMutex
}

// 用来存储 info 命令的结果
type slaveInfo struct {
	ip string
	port string
	state string
	offset int64
	lag int64
}

type masterInfo struct {
	masterIp string
	masterPort int64
	masterReplOffset int64
	masterReplId string
	masterLinkStatus string
}

func (s *SentinelServer) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	size := len(cmdLine)
	if cmdName == "sentinel" {
		if size < 2 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		key := strings.ToLower(string(cmdLine[1]))
		if key == "is-master-down-by-addr" {
			if size != 6 {
				return protocol.MakeArgNumErrReply(cmdName + "is-master-down-by-addr")
			}
			ip, portStr, _, _ := string(cmdLine[2]), string(cmdLine[3]), string(cmdLine[4]), string(cmdLine[5])
			port, err := strconv.Atoi(portStr)
			if err != nil {
				return protocol.MakeErrReply("illegal port number: " + portStr)
			}

			logger.Info("sentinel is-master-down-by-addr", ip, portStr)
			// 检查 masters 里，对应的节点的状态
			var target *SentinelRedisInstance
			s.state.Masters.ForEach(func(key string, val interface{}) bool {
				sri := val.(*SentinelRedisInstance)
				masterIP := sri.Addr.Ip
				masterPort := sri.Addr.Port

				if ip == masterIP && port == masterPort {
					target = sri
					return false
				}
				return true
			})

			if target == nil {
				return protocol.MakeStatusReply(fmt.Sprintf("%s:%d is not a master", ip, portStr))
			}

			if target.Flag == ONLINE {
				// 这里多传两个参数 "isMasterDown", target.Name, 方便接收方识别
				return protocol.MakeMultiBulkReply(utils.ToCmdLine("isMasterDown", target.Name, "0", "*", "0"))
			}

			return protocol.MakeMultiBulkReply(utils.ToCmdLine("isMasterDown", target.Name, "1", "*", "0"))
		}
	}
	return &protocol.OkReply{}
}

func (s *SentinelServer) AfterClientClose(c redis.Connection) {
	logger.Info("sentinel server AfterClientClose")
}

func (s *SentinelServer) Close() {
	for _, conn := range s.cmdConnMap {
		c, _ := conn.(*connection.Connection)
		_ = c.Close()
	}

	for _, conn := range s.subConnMap {
		c, _ := conn.(*connection.Connection)
		_ = c.Close()
	}

}

func parseSentinelStateFromConfig() *SentinelState {
	sentinelMap := config.Properties.Sentinel
	masterDict := dict.MakeConcurrent(DICT_SIZE)

	for masterName, configMap := range sentinelMap {
		sri := &SentinelRedisInstance{
			Role:            database.MasterRole,
			Flag: 			 ONLINE,
			Name:            masterName,
			DownAfterPeriod: configMap[DOWN_AFTER_PERIOD].(int),
			Quorum:          configMap[QUORUM].(int),
			Addr:            &SentinelAddr{
				Ip: configMap[IP].(string),
				Port: configMap[PORT].(int),
			},
			ParallelSyncs:   configMap[PARALLEL_SYNCS].(int),
			Slaves: dict.MakeConcurrent(DICT_SIZE),
		}
		masterDict.Put(masterName, sri)
	}
	return &SentinelState{Masters: masterDict}
}

func NewSentinelServer() *SentinelServer {
	ret := &SentinelServer{
		state: parseSentinelStateFromConfig(),
		cmdConnMap: make(map[string]redis.Connection),
		subConnMap: make(map[string]redis.Connection),

		cmdChanMap: make(map[string]<-chan *parser.Payload),
		subChanMap: make(map[string]<-chan *parser.Payload),

		selConnMap: make(map[string]redis.Connection),
		selChanMap: make(map[string]<-chan *parser.Payload),

		pingMap: make(map[string]int64),

		countMap: make(map[string]int),

		RunID: idgenerator.GenID(),
		Sentinels: dict.MakeConcurrent(SMALL_DICT_SIZE),

		// 一开始，大家都是 follower
		RaftRole: FOLLOWER,
	}
	ret.InitSentinel()
	return ret
}

func (s *SentinelServer) InitSentinel() {
	// 这里 masters 里 key 是 配置文件里的 master name
	// 而如果是 slave，key 则是 ip:port
	s.state.Masters.ForEach(func(key string, val interface{}) bool {
		sri := val.(*SentinelRedisInstance)
		masterIp := sri.Addr.Ip
		masterPort := sri.Addr.Port
		// 和每个 master 建立连接
		// 连接有两个，一个是命令连接，一个是订阅连接
		// 命令连接
		addr := fmt.Sprintf("%s:%d", masterIp, masterPort)
		logger.Info("master addr: ", addr)
		cmdConn, err := net.Dial("tcp", addr)
		if err != nil {
			logger.Warn("cannot connect to %s", addr)
		}
		//key 是 master name
		s.cmdConnMap[key] = connection.NewConn(cmdConn)
		s.cmdChanMap[key] = parser.ParseStream(cmdConn)

		// 实现订阅连接, 和 订阅 Channel
		// 把 sentinel 当成一个 client，向 master 发 subscribe _sentinel_:hello，
		// 让 master 建立一个 _sentinel_:hello -> sentinel 的映射关系
		subConn, err := net.Dial("tcp", addr)
		if err != nil {
			logger.Warn("cannot connect to %s", addr)
			return true
		}
		//key 是 master name
		s.subConnMap[key] = connection.NewConn(subConn)
		s.subChanMap[key] = parser.ParseStream(subConn)

		cmdPayload := utils.ToCmdLine(SUB_CMD, SENTINEL_CHANNEL)
		_, err = subConn.Write(protocol.MakeMultiBulkReply(cmdPayload).ToBytes())
		if err != nil {
			logger.Warn("cannot establish sub connection to master")
		}
		return true
	})

	// 根据建立的连接，
	// 		每隔 10s 给 master 和 slave 发 info 命令
	//      每隔 2s 在 _sentinel_:hello 通道上发送 sentinel addr 和 master addr
	//      每隔 1s 给所有其他节点发送 ping 命令，根据结果来标记 master 是否为主观下线状态
	logger.Info("startSentinelCron")
	s.startSentinelCron()

	// 逐个读取每个 master/slave 的 info/ping 回复
	s.receiveMasterOrSlaveResp()

	//  因为订阅了 _sentinel_:hello 频道，所以要读取该频道上的消息
	s.receiveMsgFromChannel()

	// 读取来自其他 sentinel 的回复
	s.receiveMsgFromSentinel()

}

func (s *SentinelServer) parseMsgFromChannel(payload *parser.Payload) {
	if payload.Err != nil {
		return
	}
	payloadStr := string(payload.Data.ToBytes())

	if payloadStr[0] != '*' {
		return
	}
	/*
	*3\r\n
	$7\r\n
	message\r\n
	$16\r\n
	_sentinel_:hello\r\n
	$44\r\n
	127.0.0.1,26379,2LFfPJQiB9N6yAQjKzhs8zzxQ5I\r\n
	*/
	ls := strings.Split(payloadStr, "\r\n")
	i := 2
	n := len(ls)
	if i < n && ls[2] == "message" {
		msg := ls[6]
		ls = strings.Split(msg, ",")
		ip, portStr, runID := ls[0], ls[1], ls[2]
		port, _ := strconv.Atoi(portStr)
		if ip == config.Properties.Bind && port == config.Properties.Port {

			return
		}
		// 记录其他 sentinel
		sri := &SentinelRedisInstance{
			Role:            database.SentinelRole,
			Name: fmt.Sprintf("%s:%d", ip, port),
			RunID:           runID,
			Addr:            &SentinelAddr{
				Ip:   ip,
				Port: port,
			},
		}

		_, exists := s.Sentinels.Get(sri.Name)
		// 这是一个新增的 sentinel，那么需要和这个 sentinel 建立连接
		// sentinel 之间，只建立命令连接
		if !exists {
			cmdConn, err := net.Dial("tcp", sri.Name)
			if err != nil {
				logger.Warn("cannot connect to new sentinel: %s", sri.Name)
			}

			//logger.Info("connect to a new sentinel server: ", sri.Name)
			newConn := connection.NewConn(cmdConn)
			s.selConnMap[sri.Name] = newConn
			s.selChanMap[sri.Name] = parser.ParseStream(cmdConn)
		}
		s.Sentinels.Put(sri.Name, sri)
	}
}

func (s *SentinelServer) parseMsgFromSentinel(payload *parser.Payload) {
	if payload.Err != nil {
		return
	}
	payloadStr := string(payload.Data.ToBytes())

	if payloadStr[0] != '*' {
		return
	}

	ls := strings.Split(payloadStr, "\r\n")
	nodeName := ls[4]
	isMasterDown := len(ls) > 6 && ls[6] == "1"
	if isMasterDown {
		s.countMap[nodeName]++
		quorum, ok := config.Properties.Sentinel[nodeName]["quorum"]
		if !ok {
			quorum = 2
		}
		// 标记为客观下线
		if s.countMap[nodeName] >= quorum.(int) {
			// 它是一个主节点
			raw, sriExists := s.state.Masters.Get(nodeName)
			if sriExists {
				sri := raw.(*SentinelRedisInstance)
				if sri.Flag == SRI_S_DOWN {
					sri.Flag = SRI_O_DOWN
					s.state.Masters.Put(nodeName, sri)
				}
				return
			}

			// 它是一个从节点
			s.state.Masters.ForEach(func(key string, val interface{}) bool {
				masterSri := val.(*SentinelRedisInstance)
				slaveSriRaw, slaveExists := masterSri.Slaves.Get(nodeName)
				if slaveExists {
					//  标记为客观下线
					slaveSri := slaveSriRaw.(*SentinelRedisInstance)
					if slaveSri.Flag == SRI_S_DOWN {
						slaveSri.Flag = SRI_O_DOWN
					}
					return false
				}
				return true
			})

		}

	}
}

func (s *SentinelServer) receiveMsgFromSentinel() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic", err)
			}
		}()
		for {
			for _, channel := range s.selChanMap {
				select {
				case payload := <-channel:
					if payload.Err != nil {
						logger.Info("receive master/slave response error, ", payload.Err)
					}
					// 拿到解析信息后，开始更新
					s.parseMsgFromSentinel(payload)
				default:
					continue
				}
			}
		}
	}()

}
func (s *SentinelServer) receiveMsgFromChannel() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic", err)
			}
		}()
		for {
			for _, channel := range s.subChanMap {
				select {
				case payload := <-channel:
					if payload.Err != nil {
						logger.Info("receive master/slave response error, ", payload.Err)
					}
					// 拿到解析信息后，开始更新
					s.parseMsgFromChannel(payload)
				default:
					continue
				}
			}
		}
	}()

}

// 这里用来接收 sentinel 向 master 和 slave 发出 info 命令后，收到的回答
// 或者接受 sentinel 向其他节点发送 ping 命令后，收到的回答
func (s *SentinelServer) receiveMasterOrSlaveResp() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic", err)
			}
		}()
		for {
			for name, channel := range s.cmdChanMap {
				select {
				case payload := <-channel:
					if payload.Err != nil {
						logger.Info("receive master/slave response error, ", payload.Err)
					}
					// 将 payload 当做 ping 命令回复解析
					if parsePing(payload, s, name) {
						continue
					}

					// 将 payload 当做 info 命令回复解析
					mi, si := parseInfoFromMasterOrSlave(payload)
					if si == nil || len(si) == 0 {
						// 如果这是一个 master 反馈回来的，那么就应该清除这个 master 下的所有 slave
						// 首先要判断它是不是一个 master
						raw, exists := s.state.Masters.Get(name)
						if exists {
							sri, _ := raw.(*SentinelRedisInstance)
							// 断开 sentinel 和 slave 的连接
							sri.Slaves.ForEach(func(key string, val interface{}) bool {
								slaveSri, _ := val.(*SentinelRedisInstance)
								addr := fmt.Sprintf("%s:%d", slaveSri.Addr.Ip, slaveSri.Addr.Port)
								for kaddr, cmdConn := range s.cmdConnMap {
									if kaddr == addr {
										cc, _ := cmdConn.(*connection.Connection)
										_ = cc.Close()
										delete(s.cmdConnMap, addr)

									}
								}

								for kaddr, cmdConn := range s.subConnMap {
									if kaddr == addr {
										cc, _ := cmdConn.(*connection.Connection)
										_ = cc.Close()
										delete(s.subConnMap, addr)
									}
								}
								return true
							})
							sri.Slaves.Clear()
						}
						continue
					}
					// 解析 master 返回的 slave 信息
					if si != nil && len(si) > 0 {
						raw, exists := s.state.Masters.Get(name)
						if !exists {
							logger.Info("sentinel receiveMasterResp: ", name, "not exists")
							continue
						}
						sri, _ := raw.(*SentinelRedisInstance)

						tempMap := make(map[string]bool)
						for _, slaveItem := range si {
							slaveKey := fmt.Sprintf("%s:%s", slaveItem.ip, slaveItem.port)
							tempMap[slaveKey] = true
						}

						for _, slaveItem := range si {
							slaveKey := fmt.Sprintf("%s:%s", slaveItem.ip, slaveItem.port)
							port, _ := strconv.Atoi(slaveItem.port)
							slaveSriRaw, ok := sri.Slaves.Get(slaveKey)
							// 如果这个 slave 节点是一个新增节点，那么就建立 sentinel 和 slave 的命令和订阅连接
							if !ok {
								temp := &SentinelRedisInstance{
									Role:            database.SlaveRole,
									Name:            slaveKey,
									Addr:            &SentinelAddr{
										Ip:   slaveItem.ip,
										Port: port,
									},
									Slaves:          nil,
									ParallelSyncs:   0,
								}
								sri.Slaves.Put(slaveKey, temp)
								// 建立 sentinel 和 slave 之间的 tcp 连接( 一个是命令连接，一个是订阅连接）
								logger.Info("新增一个 slave, addr: ", slaveKey)
								cmdConn, err := net.Dial("tcp", slaveKey)
								if err != nil {
									logger.Warn("cannot connect to %s", slaveKey)
									continue
								}
								// key 是 master name, 建立命令连接
								s.cmdConnMap[slaveKey] = connection.NewConn(cmdConn)
								s.cmdChanMap[slaveKey] = parser.ParseStream(cmdConn)

								// todo 建立订阅连接

							} else {
								// 在当前的时间下，这个 slave 已经和 master 断开了
								if _, exists := tempMap[slaveKey]; !exists {
									if conn, ok := s.cmdConnMap[slaveKey]; ok {
										cc, _ := conn.(*connection.Connection)
										_ = cc.Close()
										delete(s.cmdConnMap, slaveKey)
									}

									if conn, ok := s.subConnMap[slaveKey]; ok {
										cc, _ := conn.(*connection.Connection)
										_ = cc.Close()
										delete(s.subConnMap, slaveKey)
									}
									sri.Slaves.Remove(slaveKey)
								} else {
									slaveSri, _ := slaveSriRaw.(*SentinelRedisInstance)
									slaveSri.Role = database.SlaveRole
									slaveSri.Name = slaveKey
									slaveSri.Addr.Ip = slaveItem.ip
									slaveSri.Addr.Port = port
									sri.Slaves.Put(slaveKey, slaveSri)
								}
							}
						}
						s.state.Masters.Put(name, sri)
					}

					if mi != nil {
						// 解析 slave 返回的 master 信息
						// 根据 slave 返回的 master 信息（mi) 更新
						// 由于 slave 只知道 master 的 ip 和 port，但不知道配置文件中 master name，所以要遍历整个 masters 字典
						isNewMaster := true
						s.state.Masters.ForEach(func(key string, val interface{}) bool {
							masterSri, _ := val.(*SentinelRedisInstance)
							// 如果已经存在，那就不用更新了
							if mi.masterIp == masterSri.Addr.Ip && mi.masterPort == int64(masterSri.Addr.Port) {
								isNewMaster = false
								return false
							}

							// todo 删除日志打印
							logger.Info("master", "key", "有 slave ", masterSri.Slaves.Len(), " 个")
							masterSri.Slaves.ForEach(func(key string, val interface{}) bool {
								slaveSri, _ := val.(*SentinelRedisInstance)
								logger.Info("\tslave: ", key)
								logger.Info("\tip: ", slaveSri.Addr.Ip)
								logger.Info("\tport: ", slaveSri.Addr.Port)
								return true
							})
							return true
						})
						// 如果 slave 连接的是一台新的 master，那就记录下来，同时建立连接
						if isNewMaster {
							// 这里的 key 用 masterIp:materPort
							key := fmt.Sprintf("%s:%d", mi.masterIp, mi.masterPort)
							masterSri := &SentinelRedisInstance{
								Role:            database.MasterRole,
								Name:            key,
								DownAfterPeriod: 30000,
								Quorum:          1,
								Addr:            &SentinelAddr{
									Ip: mi.masterIp,
									Port: int(mi.masterPort),
								},
								Slaves:          dict.MakeConcurrent(DICT_SIZE),
								ParallelSyncs:   1,
							}

							// 建立 sentinel 和 master 之间的 tcp 连接( 一个是命令连接，一个是订阅连接）
							logger.Info("新增一个 master，addr: ", key)
							cmdConn, err := net.Dial("tcp", key)
							if err != nil {
								logger.Warn("cannot connect to new master: %s", key)
								continue
							}
							// key 是 master name, 建立命令连接
							s.cmdConnMap[key] = connection.NewConn(cmdConn)
							s.cmdChanMap[key] = parser.ParseStream(cmdConn)

							// todo 建立订阅连接


							s.state.Masters.Put(key, masterSri)
						}
					}
				default:
					continue
				}
			}
		}
	}()
}


func parsePing(payload *parser.Payload, s *SentinelServer, nodeName string) bool {
	// 表示 payload 不是对 ping 的回复
	if payload.Data.ToBytes()[0] == '*' {
		return false
	}

	payloadStr := strings.TrimSpace(strings.ToLower(string(payload.Data.ToBytes())))
	// 这里认为没有回复 pong 就是异常，进行主观下线标记
	if payloadStr != "+pong" {
		logger.Info(fmt.Sprintf("ping 回复异常: %s", payloadStr))
		s.markAsSubjectDown(nodeName)
	} else {
		// 连接正常, 如果之前存在异常，那么现在清除掉异常标记
		s.markAsOnline(nodeName)
	}
	return true
}

func parseInfoFromMasterOrSlave(payload *parser.Payload) (*masterInfo, []*slaveInfo){
	payloadStr := string(payload.Data.ToBytes())
	if payloadStr[0] != '*' {
		logger.Info("payload[0] != '*'")
		return nil, nil
	}

	ls := strings.Split(payloadStr, "\r\n")
	i := 2
	n := len(ls)
	var slaveInfos []*slaveInfo
	var mi *masterInfo
	var roleStr string
	for i=2;i<n;i+=2 {
		splitIndex := strings.Index(ls[i], ":")
		if ls[i][0] == '#' || splitIndex == -1 {
			continue
		}

		name := ls[i][:splitIndex]
		value := ls[i][splitIndex+1:]
		if name == "role" {
			roleStr = value
		}

		if roleStr == "master" {
			// 发送 info 回复的是 master 节点，那么只解析和处理和它连接的 slave 信息
			//因为这里的 info 实现只返回了 slave 信息，所以这里只处理 slave 信息，实际 redis master 节点返回很多信息给 sentinel 节点
			if strings.HasPrefix(name, "slave") {
				// ip=127.0.0.1,port=54596,state=online,offset=0,lag=63810053011
				getSlaveInfoFromMaster(name, value, &slaveInfos)
			}
		} else {
			// 发送 info 回复的是 slave 节点，那么只解析和处理和它连接的 master 信息
			if mi == nil {
				mi = &masterInfo{}
			}
			getMasterInfoFromSlave(name, value, mi)
		}
	}

	return mi, slaveInfos
}

func (s *SentinelServer) startSentinelCron() {
	// 每隔 10s 发送 info 命令
	execTask(10, s.sentinelCron)

	// 每隔 2s 在 _sentinel_:hello 频道上发送 sentinel addr 和 master addr
	execTask(2, s.sentinelPub)

	// 每隔 1s 向其他节点发送 ping 命令
	execTask(1, s.sentinelPing)

	// 每隔 10s 检查一次是否有节点是主观下线状态
	execTask(10, s.sentinelCheckNodeSubjectDown)

	// 每隔 15s 检查一次是否有节点是客观下线状态
	execTask(15, s.sentinelCheckMasterObjectDown)
}

// 执行定时任务
func execTask(interval int, task func()) {
	go func() {
		//defer func() {
		//	if err := recover(); err != nil {
		//		logger.Error("panic", err)
		//	}
		//}()
		ticker := time.Tick(time.Duration(interval) * time.Second)
		for range ticker {
			task()
		}
	}()
}

func (s *SentinelServer) sentinelCron() {
	// 给每个 master 和 slave 发 info 命令
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, conn := range s.cmdConnMap {
		infoCmdLine := utils.ToCmdLine("info")
		infoReq := protocol.MakeMultiBulkReply(infoCmdLine)
		err := conn.Write(infoReq.ToBytes())
		if err != nil {
			//msg := fmt.Sprintf("send info to master %s faild %s", addr, err.Error())
			//logger.Error(msg)
		}
	}
}

func (s *SentinelServer) sentinelPub() {
	// 通过 _sentinel_:hello 频道发送 sentinel addr 和 master addr
	message := fmt.Sprintf("%s,%d,%s", config.Properties.Bind, config.Properties.Port, s.RunID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, conn := range s.subConnMap {
		pubCmdLine := utils.ToCmdLine("publish", SENTINEL_CHANNEL, message)
		pubReq := protocol.MakeMultiBulkReply(pubCmdLine)
		err := conn.Write(pubReq.ToBytes())
		if err != nil {
			//msg := fmt.Sprintf("publish message to master %s faild %s", addr, err.Error())
			//logger.Error(msg)
		}
	}
}

func (s *SentinelServer) markAsOnline(nodeName string) {
	delete(s.pingMap, nodeName)
	raw, sriExists := s.state.Masters.Get(nodeName)
	if !sriExists {
		return
	}
	//  标记为在线
	sri := raw.(*SentinelRedisInstance)
	if sri.Flag == ONLINE {
		return
	}
	sri.Flag = ONLINE
	s.state.Masters.Put(nodeName, sri)
}

func (s *SentinelServer) markAsSubjectDown(nodeName string) {
	t, exists := s.pingMap[nodeName]
	downAfterMS, ok := config.Properties.Sentinel[nodeName]["down-after-milliseconds"].(int64)
	if !ok {
		// 如果配置文件里没配置，则使用默认值
		downAfterMS = 30000
	}
	// 表示之前就出现过异常
	if exists {
		now := time.Now().UnixMilli()
		// 表示从首次异常开始，downAfterMS 毫秒内，第二次出现异常
		// 现在我们认为在 downAfterMS 内，nodeName 连续两次异常，将它标记为主观下线
		if now < t + downAfterMS {
			// 找到它的 sri
			raw, sriExists := s.state.Masters.Get(nodeName)
			// 它是一个主节点
			if sriExists {
				//  标记为主观下线
				sri := raw.(*SentinelRedisInstance)
				if sri.Flag == ONLINE {
					sri.Flag = SRI_S_DOWN
					s.countMap[nodeName] = 1
					s.state.Masters.Put(nodeName, sri)
				}
				return
			}

			// 它是一个从节点
			s.state.Masters.ForEach(func(key string, val interface{}) bool {
				masterSri := val.(*SentinelRedisInstance)
				slaveSriRaw, slaveExists := masterSri.Slaves.Get(nodeName)
				if slaveExists {
					//  标记为主观下线
					slaveSri := slaveSriRaw.(*SentinelRedisInstance)
					if slaveSri.Flag == ONLINE {
						slaveSri.Flag = SRI_S_DOWN
					}
					return false
				}
				return true
			})
			return
		}
	}
	s.pingMap[nodeName] = time.Now().UnixMilli()
}

// 从 master 里删除 ping 超时的 slave, 关闭和清理该 slave 相关的连接
func (s *SentinelServer) deleteSlave(nodeName string) {
	s.state.Masters.ForEach(func(key string, val interface{}) bool {
		sri := val.(*SentinelRedisInstance)
		sri.Slaves.Remove(nodeName)
		return true
	})

	s.mu.Lock()
	defer s.mu.Unlock()

	if c, ok := s.cmdConnMap[nodeName]; ok {
		cc := c.(*connection.Connection)
		_ = cc.Close()
	}
	delete(s.cmdConnMap, nodeName)

	if c, ok := s.subConnMap[nodeName]; ok {
		cc := c.(*connection.Connection)
		_ = cc.Close()
	}
	delete(s.subConnMap, nodeName)


}

func (s *SentinelServer) sentinelPing() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for addr, conn := range s.cmdConnMap {
		pingCmdLine := utils.ToCmdLine("ping")
		pingReq := protocol.MakeMultiBulkReply(pingCmdLine)
		err := conn.Write(pingReq.ToBytes())
		if err != nil {
			// 将对应的 master/slave 标记为主观下线
			s.markAsSubjectDown(addr)
			//msg := fmt.Sprintf("send ping to master/slave %s faild %s", addr, err.Error())
			//logger.Error(msg)
		} else {
			s.markAsOnline(addr)
		}
	}

	for addr, conn := range s.selConnMap {
		pingCmdLine := utils.ToCmdLine("ping")
		pingReq := protocol.MakeMultiBulkReply(pingCmdLine)
		err := conn.Write(pingReq.ToBytes())
		if err != nil {
			msg := fmt.Sprintf("send ping to sentinel %s faild %s", addr, err.Error())
			logger.Error(msg)
		}
	}
}

func (s *SentinelServer) sentinelCheckMasterObjectDown() {
	var downMaster, newMaster *SentinelRedisInstance
	var downMasterName string
	var otherSlavesAddr []string
	s.state.Masters.ForEach(func(key string, val interface{}) bool {
		sri := val.(*SentinelRedisInstance)
		if sri.Flag == SRI_O_DOWN {
			downMaster = sri
			downMasterName = key
			return false
		}
		return true
	})
	// 从 down 掉的 master 中找一台在线的 slave，把这台 slave 变成 master, 并对所有 slave 节点执行 slaveof no one
	if downMaster != nil {
		downMsg := fmt.Sprintf("检测到 %s:%d 客观下线", downMaster.Addr.Ip, downMaster.Addr.Port)
		logger.Info(downMsg)

		downMaster.Slaves.ForEach(func(key string, val interface{}) bool {
			slaveSri := val.(*SentinelRedisInstance)
			if slaveSri.Flag == ONLINE {
				//  给 slave 发送 slaveof no one
				cmdLine := utils.ToCmdLine("slaveof", "no", "one")
				err := s.cmdConnMap[slaveSri.Name].Write(protocol.MakeMultiBulkReply(cmdLine).ToBytes())
				if err != nil {
					msg := fmt.Sprintf("send slaveof no one to %s failed", slaveSri.Name)
					logger.Error(msg)
					return true
				}
				if newMaster == nil {
					newMaster = slaveSri
				} else {
					otherSlavesAddr = append(otherSlavesAddr, slaveSri.Name)
				}
			}
			return true
		})

		// 让其余 slave 执行 slaveof newMasterIP newMasterPort
		for _, addr := range otherSlavesAddr {
			cmdLine := utils.ToCmdLine("slaveof", newMaster.Addr.Ip, fmt.Sprintf("%d", newMaster.Addr.Port))
			err := s.cmdConnMap[addr].Write(protocol.MakeMultiBulkReply(cmdLine).ToBytes())
			if err != nil {
				msg := fmt.Sprintf("send slaveof %s %d to %s failed", newMaster.Addr.Ip, newMaster.Addr.Port, newMaster.Name)
				logger.Error(msg)
			}
		}

		// 把这台下线 master 从 sentinel 里删除
		s.state.Masters.Remove(downMasterName)
		s.mu.Lock()
		delete(s.cmdConnMap, downMasterName)
		delete(s.subConnMap, downMasterName)
		delete(s.cmdChanMap, downMasterName)
		delete(s.subChanMap, downMasterName)
		delete(s.pingMap, downMasterName)
		delete(s.countMap, downMasterName)

		// 更新 sentinel 配置
		config.Properties.Sentinel[newMaster.Name] = make(map[string]interface{})
		for k, v := range config.Properties.Sentinel[downMasterName] {
			config.Properties.Sentinel[newMaster.Name][k] = v
		}
		// 只有 ip 和 port 要改，其余继承自 oldMaster
		config.Properties.Sentinel[newMaster.Name]["ip"] = newMaster.Addr.Ip
		config.Properties.Sentinel[newMaster.Name]["port"] = newMaster.Addr.Port
		delete(config.Properties.Sentinel, downMasterName)
		s.mu.Unlock()
	}
}

func (s *SentinelServer) sentinelCheckNodeSubjectDown() {
	s.state.Masters.ForEach(func(key string, val interface{}) bool {
		sri := val.(*SentinelRedisInstance)
		// 如果有 master 被标记为主观下线，那么开始检查是否客观下线
		if sri.Flag == SRI_S_DOWN {
			// 有一种特殊情况，只有一个 sentinel，那么这种情况下，直接标记为客观下线
			if s.Sentinels.Len() == 0 {
				sri.Flag = SRI_O_DOWN
			} else {
				// 开始询问其他 sentinel
				s.sendIsNodeDown(sri)
			}
		} else if sri.Flag == ONLINE {
			// 对于在线的主节点，检查他们的 slave 节点，是否有标记为主观下线的状态
			var sris []*SentinelRedisInstance
			sri.Slaves.ForEach(func(key string, val interface{}) bool {
				slaveSri := val.(*SentinelRedisInstance)
				if slaveSri.Flag == SRI_S_DOWN {
					logger.Info(fmt.Sprintf("发现 slave %s:%d 处于主观下线状态", slaveSri.Addr.Ip, slaveSri.Addr.Port))
					sris = append(sris, slaveSri)
				}
				return true
			})

			for _, item := range sris {
				s.sendIsNodeDown(item)
			}
		}
		return true
	})

}

func (s *SentinelServer) sendIsNodeDown(sri *SentinelRedisInstance) {
	// 开始询问其他 sentinel
	s.Sentinels.ForEach(func(key string, val interface{}) bool {
		sentinelSri := val.(*SentinelRedisInstance)
		portStr := strconv.Itoa(sri.Addr.Port)
		cmdLine := utils.ToCmdLine("sentinel", "is-master-down-by-addr", sri.Addr.Ip, portStr, "0", sri.RunID)
		senConn, ok := s.selConnMap[sentinelSri.Name]
		if !ok {
			return true
		}
		err := senConn.Write(protocol.MakeMultiBulkReply(cmdLine).ToBytes())
		if err != nil {
			logger.Error("ask other sentinel for master down failed")
		}
		return true
	})
}

func getSlaveInfoFromMaster(name, value string, slaveInfos *[]*slaveInfo) {
	// ip=127.0.0.1,port=54596,state=online,offset=0,lag=63810053011
	var err error
	si := &slaveInfo{}
	slaveInfoList := strings.Split(value, ",")
	//logger.Info("len(slaveInfoList): ", len(slaveInfoList))
	if len(slaveInfoList) < 5 {
		return
	}
	for _, item := range slaveInfoList {
		itemList := strings.Split(item, "=")
		if itemList[0] == "ip" {
			si.ip = itemList[1]
		} else if itemList[0] == "port" {
			si.port = itemList[1]
		} else if itemList[0] == "state" {
			si.state = itemList[1]
		} else if itemList[0] == "offset" {
			si.offset, err = strconv.ParseInt(itemList[1], 10, 64)
			if err != nil {
				si.offset = 0
			}
		} else if itemList[0] == "lag" {
			si.lag, err = strconv.ParseInt(itemList[1], 10, 64)
			if err != nil {
				si.lag = 0
			}
		}
	}
	*slaveInfos = append(*slaveInfos, si)
}

func getMasterInfoFromSlave(name, value string, mi *masterInfo) {
	if name == "master_host" {
		mi.masterIp = value
	} else if name == "master_port" {
		mi.masterPort, _ = strconv.ParseInt(value, 10, 64)
	} else if name == "master_link_status" {
		mi.masterLinkStatus = value
	}
}