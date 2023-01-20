package database

import (
	"github.com/hodis/config"
	"github.com/hodis/interface/redis"
	"github.com/hodis/redis/protocol"
	"regexp"
	"strings"
)

func Ping(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

//config get parameter [parameter...]
func execConfig(db *DB, args [][]byte) redis.Reply {
	operation := strings.ToLower(string(args[0]))
	parameters := args[1:]
	allConfig := config.ReadAllConfig()
	var ret [][]byte
	if operation == "get" {
		// 获取所有配置
		if string(parameters[0]) == "*" {
			for k, v := range allConfig {
				ret = append(ret, []byte(k), []byte(v))
			}
			return protocol.MakeMultiBulkReply(ret)
		}

		for _, para := range parameters {
			paraStr := strings.ToLower(string(para))
			// 当成正则表达式处理，找出 allConfig 里所有满足该表达式的 key, 以及对应的 value
			if strings.Contains(paraStr, "*") {
				p, err := regexp.Compile(paraStr)
				if err != nil {
					continue
				}
				for k, v := range allConfig {
					if p.MatchString(k) {
						ret = append(ret, []byte(k), []byte(v))
					}
				}
			} else {
				// 当成普通字符串进行匹配
				_, exists := allConfig[paraStr]
				if exists {
					ret = append(ret, []byte(paraStr), []byte(allConfig[paraStr]))
				}
			}
		}
		return protocol.MakeMultiBulkReply(ret)
	} else {
		// 暂时只支持 config get, 其他的不支持
		return protocol.MakeEmptyMultiBulkReply()
	}
}

func init() {
	RegisterCommand("Ping", Ping, noPrepare, nil, -1, flagReadOnly)
}