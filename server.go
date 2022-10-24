package main

// 用 go 实现 redis
// https://www.cnblogs.com/Finley/category/1598973.html

import (
	"fmt"
	"log"
	"net"
)

func ListenAndServe(address string) {
	// 绑定监听地址
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(fmt.Sprintf("listen err: %+v", err))
	}

	defer listener.Close()

	log.Println(fmt.Sprintf("bind: %s, start listening...", address))

	for {
		conn, connErr := listener.Accept()
		if connErr != nil {
			log.Fatal(fmt.Sprintf("accept err: %v", err))
		}
		// 使用协程来处理新连接
		Handler(conn)
	}
}

func Handler(conn net.Conn) {
	// reader := bufio.NewReader(conn)

}

func main() {
	ListenAndServe(":8888")
}
