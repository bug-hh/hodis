package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/hodis/lib/logger"
	"github.com/hodis/redis/client"
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
	CLIENT_DISPLAY = "127.0.0.1:30000> "
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

/*
把用户输入的字符串命令转化为二维 byte 数组
 */
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

/*
阻塞式 redis-client
 */
func useClient1() {
	addr := "127.0.0.1:6399"
	conn, err := net.Dial("tcp", addr)
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
		n, readErr := conn.Read(buf[:])
		if readErr != nil {
			fmt.Println("recv failed, err:", readErr)
			return
		}
		fmt.Println(UnWrapReply(buf[:n]))
	}
}

// 使用非阻塞式 tcp client，pipeline 模式的 redis-client
// 这种在服务端未响应时客户端继续向服务端发送请求的模式称为 Pipeline 模式
func useClient2() {
	addr := "127.0.0.1:30000"
	inputReader := bufio.NewReader(os.Stdin)
	redisClient, err := client.MakeClient(addr)
	redisClient.Start()
	if err != nil {
		logger.Error("err : ", err)
		return
	}
	for {
		fmt.Printf(CLIENT_DISPLAY)
		input, _ := inputReader.ReadString('\n') // 读取用户输入
		if input == "\n" {
			continue
		}

		if strings.ToUpper(strings.TrimSpace(input)) == "QUIT" { // 如果输入 quit 就退出
			return
		}
		result := redisClient.Send(handleCMD2(input))
		if result != nil {
			fmt.Println(UnWrapReply(result.ToBytes()))
		} else {
			fmt.Println("result is nil")
		}

	}
}

// TCP 客户端
func main() {
	useClient2()
}


