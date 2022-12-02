package cluster

import (
	"bytes"
	"encoding/base64"
	"github.com/hodis/interface/database"
	"github.com/hodis/redis/parser"
	"github.com/hodis/redis/protocol"
)

/*
对传入所有命令做 base64 编码
base64 编码的作用：因为某些场合并不能传输或者储存二进制流，例如 一个传输协议是基于ASCII文本的，那么它就不能传输二进制流
 */
func encodeCmdLine(cmdLines []database.CmdLine) [][]byte {
	var result [][]byte
	for _, line := range cmdLines {
		raw := protocol.MakeMultiBulkReply(line).ToBytes()
		encoded := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
		base64.StdEncoding.Encode(encoded, raw)
		result = append(result, encoded)
	}
	return result
}

// base64 解码
func parseEncodedMultiRawReply(args [][]byte) (*protocol.MultiRawReply, error) {
	cmdBuf := new(bytes.Buffer)
	for _, arg := range args {
		dbuf := make([]byte, base64.StdEncoding.DecodedLen(len(arg)))
		n, err := base64.StdEncoding.Decode(dbuf, arg)
		if err != nil {
			continue
		}
		cmdBuf.Write(dbuf[:n])
	}
	cmds, err := parser.ParseBytes(cmdBuf.Bytes())
	if err != nil {
		return nil, protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeMultiRawReply(cmds), nil
}