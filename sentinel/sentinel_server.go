package sentinel

import (
	"context"
	"fmt"
	"github.com/hodis/config"
	"github.com/hodis/database"
	"github.com/hodis/datastruct/dict"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/connection"
	"github.com/hodis/redis/parser"
	"github.com/hodis/redis/protocol"
	"net"
	"strconv"
	"strings"
	"time"
)

//sentinel 配置文件解析：https://www.jianshu.com/p/d237c7210dbd

const (
	DICT_SIZE = 1 << 16
	QUORUM = "quorum"
	DOWN_AFTER_PERIOD = "down_after_milliseconds"
	MASTER_NAME = "master_name"
	IP = "ip"
	PORT = "port"
	PARALLEL_SYNCS = "parallel_syncs"
)

type SentinelState struct {
	Masters dict.Dict
}

type SentinelRedisInstance struct {
	// 表示该节点是 master 还是 slave
	Role int

	// 节点名字, 一般是 ip:port
	Name string
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

	ctx context.Context
	cancel context.CancelFunc
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
	return nil
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
			return true
		}
		//key 是 master name
		s.cmdConnMap[key] = connection.NewConn(cmdConn)
		s.cmdChanMap[key] = parser.ParseStream(cmdConn)

		// todo 以后实现订阅连接, 和 订阅 Channel


		return true
	})

	// 根据建立的连接，每隔 10s 给 master 和 slave 发 info 命令
	logger.Info("startSentinelCron")
	s.startSentinelCron()

	// 逐个读取每个 master/slave 的 info 回复
	s.receiveMasterOrSlaveResp()
}

// 这里用来接收 sentinel 向 master 和 slave 发出 info 命令后，收到的回答
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
					logger.Info("收到来自 ", name, " 的 info 信息")
					// 拿到解析信息后，开始更新
					mi, si := parseInfoFromMasterOrSlave(payload)
					if si == nil || len(si) == 0 {
						// 如果这是一个 master 反馈回来的，那么就应该清除这个 master 下的所有 slave
						// 首先要判断它是不是一个master
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
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic", err)
			}
		}()
		ticker := time.Tick(10 * time.Second)
		for range ticker {
			s.sentinelCron()
		}
	}()

}

func (s *SentinelServer) sentinelCron() {
	// 给每个 master 和 slave 发 info 命令
	for addr, conn := range s.cmdConnMap {
		infoCmdLine := utils.ToCmdLine("info")
		infoReq := protocol.MakeMultiBulkReply(infoCmdLine)
		err := conn.Write(infoReq.ToBytes())
		if err != nil {
			msg := fmt.Sprintf("send info to master %s faild %s", addr, err.Error())
			logger.Error(msg)
		}
	}
}


func getSlaveInfoFromMaster(name, value string, slaveInfos *[]*slaveInfo) {
	// ip=127.0.0.1,port=54596,state=online,offset=0,lag=63810053011
	var err error
	si := &slaveInfo{}
	slaveInfoList := strings.Split(value, ",")
	logger.Info("len(slaveInfoList): ", len(slaveInfoList))
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