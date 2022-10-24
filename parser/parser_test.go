package parser

import (
	"bytes"
	"io"
	"testing"

	"github.com/hodis/lib/utils"
	"github.com/hodis/redis"
	"github.com/hodis/redis/protocol"
)

func TestParseStream(t *testing.T) {
	replies := []redis.Reply{
		protocol.MakeIntReply(1),
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrReply("ERR unknown"),
		protocol.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		protocol.MakeNullBulkReply(),
		protocol.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		protocol.MakeEmptyMultiBulkReply(),
	}

	reqs := bytes.Buffer{}

	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}

	reqs.Write([]byte("set a a" + protocol.CRLF))

	expected := make([]redis.Reply, len(replies))
	copy(expected, replies)

	expected = append(expected, protocol.MakeMultiBulkReply([][]byte{
		[]byte("set"), []byte("a"), []byte("a"),
	}))

	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++

		if !utils.BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			t.Error("parse failed: " + string(exp.ToBytes()))
		}

	}

}
