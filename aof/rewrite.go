package aof

import (
	"github.com/hodis/config"
	"github.com/hodis/interface/database"
	"github.com/hodis/lib/logger"
	"github.com/hodis/lib/utils"
	"github.com/hodis/redis/protocol"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

type RewriteCtx struct {
	tmpFile *os.File
	fileSize int64
	dbIdx int
}

func (handler *Handler) newRewriteHandler() *Handler {
	h := &Handler{}
	h.aofFilename = handler.aofFilename
	// 这里重新克隆了一个空数据库
	h.db = handler.tmpDBMaker()
	return h
}

// 在开始 aof 文件重写之前，做的准备工作
func (handler *Handler) StartRewrite() (*RewriteCtx, error) {
	// 直接用写锁，把 aof file 锁住，让其他协程无法往 aof file 里写东西
	// 但是不妨碍主协程把命令往 aofChan 里写
	// 这个时候，会造成命令在 aofChan 里堆积
	handler.pausingAof.Lock()
	defer handler.pausingAof.Unlock()

	err := handler.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	fileInfo, _ := os.Stat(handler.aofFilename)
	fileSize := fileInfo.Size()

	// 创建临时文件, 这个文件会变成新的 aof 文件
	file, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}

	return &RewriteCtx{
		tmpFile: file,
		fileSize: fileSize,
		dbIdx: handler.currentDB,
	}, nil
}

func (handler *Handler) Rewrite() error {
	// 先进行 rewrite 的准备工作
	ctx, err := handler.StartRewrite()
	if err != nil {
		logger.Info("startRewrite error: ", err)
		return err
	}
	logger.Info("startRewrite success")
	err = handler.DoRewrite(ctx)
	if err != nil {
		return err
	}

	// 首尾工作，把在重写期间新增的命令，都写到新的 aof 文件中
	// 用新的 aof 文件替换原有的 aof 文件
	handler.FinishRewrite(ctx)
	return nil
}

func (handler *Handler) DoRewrite(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile

	// 重新克隆了一个空数据库
	tmpAof := handler.newRewriteHandler()
	// 然后按照 aof 文件，往这个空数据库里写数据
	tmpAof.LoadAof(int(ctx.fileSize))

	for i := 0; i < config.Properties.Databases; i++ {
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)

		if err != nil {
			return err
		}

		// 遍历这个克隆数据库，重新生成 aof 文件
		tmpAof.db.ForEach(i, func(key string, data *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, data)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			// 带有过期时间的命令，用 PEXPIREAT 毫秒数 改写
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

func (handler *Handler) FinishRewrite(ctx *RewriteCtx) {
	handler.pausingAof.Lock()
	defer handler.pausingAof.Unlock()

	tmpFile := ctx.tmpFile

	// write commands executed during rewriting to tmp file
	src, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error("open aofFilename failed: " + err.Error())
		return
	}

	defer func() {
		_ = src.Close()
	}()

	_, err = src.Seek(ctx.fileSize, 0)
	if err != nil {
		logger.Error("seek failed: " + err.Error())
		return
	}

	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
	_, err = tmpFile.Write(data)

	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return
	}

	//copy data
	_, err = io.Copy(tmpFile, src)
	if err != nil {
		logger.Error("copy aof filed failed: " + err.Error())
		return
	}

	//用这个 tmp file 来替换原有的 aof 文件，完成 aof 重写
	_ = handler.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), handler.aofFilename)

	// reopen aof file for further write
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	handler.aofFile = aofFile

	// write select command again to ensure aof file has the same db index with  handler.currentDB
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(handler.currentDB))).ToBytes()
	_, err = handler.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}