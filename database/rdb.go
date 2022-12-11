package database

import (
	"bytes"
	"encoding/base64"
	"github.com/hdt3213/rdb/core"
	rdb "github.com/hdt3213/rdb/parser"
	"github.com/hodis/config"
	"github.com/hodis/datastruct/dict"
	list2 "github.com/hodis/datastruct/list"
	"github.com/hodis/datastruct/sortedset"
	"github.com/hodis/interface/database"
	"github.com/hodis/interface/redis"
	"github.com/hodis/lib/logger"
	"github.com/hodis/redis/protocol"
	"io"
	"os"
)

func sendRDBFile(conn redis.Connection) error {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		logger.Error("open rdb file failed " + err.Error())
		return err
	}

	defer func() {
		_ = rdbFile.Close()
	}()
	buffer := &bytes.Buffer{}
	_, copyErr := io.Copy(buffer, rdbFile)
	if copyErr != nil {
		return copyErr
	}
	bs := buffer.Bytes()
	// 把 rdb 文件做 base64 编码，当一个字符串发出去
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(bs)))
	base64.StdEncoding.Encode(encoded, bs)
	sendErr := conn.Write(protocol.MakeRDBFileReply(encoded).ToBytes())
	if sendErr != nil {
		logger.Info("send rdb error: ", sendErr.Error())
		return sendErr
	}
	logger.Info("send rdb file success")
	return nil
}

func loadRdbFile(mdb *MultiDB) {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		logger.Error("open rdb file failed " + err.Error())
		return
	}

	defer func() {
		_ = rdbFile.Close()
	}()
	// todo 自己实现 rdb 解析器
	decoder := rdb.NewDecoder(rdbFile)
	err = dumpRDB(decoder, mdb)
	if err != nil {
		logger.Error("dump rdb file failed " + err.Error())
		return
	}
}

func dumpRDB(dec *core.Decoder, mdb *MultiDB) error {
	return dec.Parse(func(o rdb.RedisObject) bool {
		db := mdb.mustSelectDB(o.GetDBIndex())
		switch o.GetType() {
		case rdb.StringType:
			str := o.(*rdb.StringObject)
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: str.Value,
			})
		case rdb.ListType:
			listObj := o.(*rdb.ListObject)
			list := list2.NewQuickList()
			for _, v := range listObj.Values {
				list.Add(v)
			}
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: list,
			})
		case rdb.HashType:
			hashObj := o.(*rdb.HashObject)
			hash := dict.MakeSimple()
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: hash,
			})
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zSet := sortedset.Make()
			for _, e := range zsetObj.Entries {
				zSet.Add(e.Member, e.Score)
			}
			db.PutEntity(o.GetKey(), &database.DataEntity{
				Data: zSet,
			})
		}
		if o.GetExpiration() != nil {
			db.Expire(o.GetKey(), *o.GetExpiration())
		}
		return true
	})
}