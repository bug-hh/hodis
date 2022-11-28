package main

import (
	"bytes"
	"fmt"
	"github.com/hodis/redis/parser"
	"strings"
)

const (
	CRLF = "\r\n"
	FLAG_MULTI_STRING = "$"
	FLAG_ARRAY = "*"
	FLAG_NUMBER = ":"
	FLAG_SIMPLE_STRING = "+"
)

//来自客户端的请求均为数组格式，它在第一行中标记报文的总行数并使用CRLF作为分行符。
func handleCMD(s string) []byte {
	//n := len(s)
	/*
		先把首尾换行符去掉
	*/
	s = strings.TrimSpace(s)
	// 按照空格作为分隔符分割
	sArr := strings.Fields(s)
	n := len(sArr)
	bs := bytes.Buffer{}
	bs.WriteString(fmt.Sprintf("%s%d%s", FLAG_ARRAY, n, CRLF))
	for _, item := range sArr {
		bs.WriteString(fmt.Sprintf("%s%d%s", FLAG_MULTI_STRING, len(item), CRLF))
		bs.WriteString(item)
		bs.WriteString(CRLF)
	}

	return bs.Bytes()
}

func main() {
	bs := []byte{
		42, 52, 13, 10, 43, 79, 75, 13, 10,
		43, 79, 75, 13, 10, 42, 49, 13, 10,
		36, 49, 13, 10, 118, 13, 10, 42, 51,
		13, 10, 36, 49, 13, 10, 49, 13, 10,
		36, 49, 13, 10, 50, 13, 10, 36, 49,
		13, 10, 51, 13, 10,
	}

	r := bytes.NewReader(bs)

	ch := make(chan *parser.Payload, 100)
	parser.Parse00(r, ch)
	for payload := range ch {
		if payload.Err != nil {
			fmt.Printf("payload 发生错误: %+v\n", payload.Err)
			break
		}

		if payload.Data == nil {
			fmt.Printf("payload empty data\n")
			continue
		}

		fmt.Printf("payload data: \n%s\n", string(payload.Data.ToBytes()))
	}

}
