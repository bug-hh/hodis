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

	ParallelSyncs int
}

// 保存被 sentinel 监控的实例的 ip 和 port
type SentinelAddr struct {
	Ip string
	Port int
}

type SentinelServer struct {
	state *SentinelState
	// 添加对 master 的 connection
	// 连接有两种，一种是命令连接，一种是订阅连接
	cmdConnMap map[string]redis.Connection
	subConnMap map[string]redis.Connection

	// 对应以上两个连接的回复 channel
	cmdChanMap map[string]<-chan *parser.Payload
	subChanMap map[string]<-chan *parser.Payload

	ctx context.Context
	cancel context.CancelFunc
}

func (s SentinelServer) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
	return nil
}

func (s SentinelServer) AfterClientClose(c redis.Connection) {
	panic("implement me")
}

func (s SentinelServer) Close() {
	panic("implement me")
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
	s.state.Masters.ForEach(func(key string, val interface{}) bool {
		sri := val.(*SentinelRedisInstance)
		masterIp := sri.Addr.Ip
		masterPort := sri.Addr.Port
		// 和每个 master 建立连接
		// 连接有两个，一个是命令连接，一个是订阅连接
		// 命令连接
		addr := fmt.Sprintf("%s:%d", masterIp, masterPort)
		cmdConn, err := net.Dial("tcp", addr)
		if err != nil {
			logger.Warn("cannot connect to %s", addr)
			return true
		}

		s.cmdConnMap[addr] = connection.NewConn(cmdConn)
		s.cmdChanMap[addr] = parser.ParseStream(cmdConn)

		// todo 以后实现订阅连接, 和 订阅 Channel

		return true
	})

	// 根据建立的连接，每隔 10s 给 master 发 info 命令
	s.startSentinelCron()

	// 逐个读取每个 master 的 info 回复
	s.receiveMasterResp()
}

func (s *SentinelServer) receiveMasterResp() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic", err)
			}
		}()
		for {
			for _, channel := range s.cmdChanMap {
				select {
				case payload := <-channel:
					if payload.Err != nil {
						logger.Info("receive master response error, ", payload.Err)
					}
					// todo 拿到 info 回复后，解析得到 slave 信息
					logger.Info("master info: ", string(payload.Data.ToBytes()))
				default:
					continue
				}
			}
		}
	}()
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
	// 给每个 master 发 info 命令
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

