package main

import (
	"bytes"
	"fmt"
	"github.com/hodis/datastruct/rax"
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
	r := rax.NewRax()
	keys := []string{
		"abcd",
		"cd",
		"XYWZ",
		"ABCD",
		//"ABCD",
		//"abx",
		//"aby",
		//"ack",
		//"acz",
		//"adl",
		//"adw",
	}
	// todo rax.numele rax.numnodes
	for i, key := range keys {
		rax.RaxInsert(r, &key, i)
		//fmt.Printf("%s\n", key)
	}
	rax.RaxShow(r)
	fmt.Printf("numele: %d\n", r.NumEle)
}
