package main

import (
	"bufio"
	"fmt"
	"io"
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
		go Handler(conn)
	}
}

func Handler(conn net.Conn) {
	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("connection closed")
			} else {
				log.Println(err)
			}
			return
		}
		b := []byte(msg)
		conn.Write(b)
	}
}

func main() {
	ListenAndServe(":8888")
}
