package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/hodis/redis/protocol"
	"os"
	"strings"
)

const (
	CRLF = "\r\n"
	FLAG_MULTI_STRING = "$"
	FLAG_ARRAY = "*"
	FLAG_NUMBER = ":"
	FLAG_SIMPLE_STRING = "+"
	FLAG_ERROR = "-"
	CLIENT_DISPLAY = "127.0.0.1:6399> "
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

func handleCMD2(s string) [][]byte {
	/*
		先把首尾换行符去掉
	*/
	s = strings.TrimSpace(s)
	// 按照空格作为分隔符分割
	sArr := strings.Fields(s)
	n := len(sArr)

	ret := make([][]byte, 0, n)
	for _, item := range sArr {
		ret = append(ret, []byte(item))
	}

	return ret
}

func main() {
	inputReader := bufio.NewReader(os.Stdin)
	input, _ := inputReader.ReadString('\n') // 读取用户输入

	bs1 := handleCMD(input)
	bs2 := protocol.MakeMultiBulkReply(handleCMD2(input)).ToBytes()

	fmt.Printf("%+v\n", bs1)
	fmt.Printf("%+v\n", bs2)
}

