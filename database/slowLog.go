package database

import (
	"bytes"
	"github.com/hodis/config"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/redis/protocol"
	"strconv"
	"time"
)

type SlowLogEntry struct {
	id int
	timestamp int64
	duration int64
	argv []string
	argc int
}


// 处理慢日志查询
func (mdb *MultiDB) handleSlowLog(startTime int64, endTime int64, cmdLine database.CmdLine) {
	// 先检查是否有慢查询配置
	if config.Properties.SlowLogLogSlowerThan < 0 {
		return
	}

	// 如果执行时间超过服务器配置时间，就写入慢查询日志
	duration := endTime - startTime
	if duration > config.Properties.SlowLogLogSlowerThan {
		entry := &SlowLogEntry{
			id: mdb.slowLogEntryId,
			timestamp: time.Now().UnixMicro(),
			duration: duration,
			argc: len(cmdLine),
		}

		for _, cl := range cmdLine {
			entry.argv = append(entry.argv, string(cl))
		}
		mdb.slowLogList.Add(entry)
		mdb.slowLogEntryId++
	}
	// 如果查询日志长度超过服务器配置，就删除部分日志
	for mdb.slowLogList.Len() > mdb.slowLogMaxLen {
		mdb.slowLogList.RemoveFirst()
	}
}

func (mdb *MultiDB) getSlowLog() redis.Reply {
	bs := bytes.Buffer{}
	mdb.slowLogList.ForEach(func(i int, v interface{}) bool {
		entry := v.(*SlowLogEntry)
		bs.WriteString(strconv.Itoa(entry.id))
		bs.WriteString("\n")
		bs.WriteString(strconv.Itoa(entry.timestamp))
		bs.WriteString("\n")
		bs.WriteString(strconv.FormatInt(entry.duration, 10))
		bs.WriteString("\n")

		for _, arg := range entry.argv {
			bs.WriteString(arg)
			bs.WriteString("\n")
		}
		return true
	})
	return protocol.MakeStatusReply(bs.String())
}