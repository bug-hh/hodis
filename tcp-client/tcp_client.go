package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
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

func toByte(s string) byte {
	return s[0]
}

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

func handleCMDTest() {
	s := handleCMD("set world godis")
	for _, item := range s {
		fmt.Printf("%02x ", item)
	}
	fmt.Printf("\n")
}

func UnWrapReply(bs []byte) string {
	n := len(bs)
	if n == 0 {
		return "Empty Reply"
	}
	if bs[0] == toByte(FLAG_SIMPLE_STRING) {
		return strings.TrimSpace(string(bs[1:]))
	}
	if bs[0] == toByte(FLAG_MULTI_STRING) {
		//$5\R\NVALUE\R\N
		sArr := strings.Split(string(bs), "\r\n")
		return sArr[1]
	}
	if bs[0] == toByte(FLAG_NUMBER) {
		return string(bs[1:n-2])
	}
	if bs[0] == toByte(FLAG_ARRAY) {
		// *3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
		//0 1   2   3  4  5  6
		//3 $3 set $3 key $5 value
		var ret []string
		sArr := strings.Split(string(bs[1:]), "\r\n")
		for i, item := range sArr {
			if i % 2 == 0 {
				ret = append(ret, item)
			}
		}
		return strings.Join(ret, " ")
	}
	if bs[0] == toByte(FLAG_ERROR) {
		return strings.TrimSpace(string(bs[1:]))
	}

	return "reply with unsupported type"
}

// TCP 客户端
func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:6399")
	if err != nil {
		fmt.Println("err : ", err)
		return
	}
	defer conn.Close() // 关闭TCP连接
	inputReader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf(CLIENT_DISPLAY)
		input, _ := inputReader.ReadString('\n') // 读取用户输入
		if input == "\n" {
			continue
		}

		if strings.ToUpper(strings.TrimSpace(input)) == "QUIT" { // 如果输入 quit 就退出
			return
		}
		_, err = conn.Write(handleCMD(input)) // 发送数据
		if err != nil {
			return
		}
		buf := [512]byte{}
		n, err := conn.Read(buf[:])
		if err != nil {
			fmt.Println("recv failed, err:", err)
			return
		}
		fmt.Println(UnWrapReply(buf[:n]))
	}
}


