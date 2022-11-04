package main

import (
	"bufio"
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
	bs := bytes.Buffer{}
	input := "set key value"

	bs.Write(handleCMD(input))
	r := bufio.NewReader(bytes.NewReader(bs.Bytes()))

	ch := make(chan *parser.Payload)
	go parser.Parse00(r, ch)
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
