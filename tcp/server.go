package tcp

// 用 go 实现 redis
// https://www.cnblogs.com/Finley/category/1598973.html

import (
	"context"
	"fmt"
	"github.com/hodis/config"
	"github.com/hodis/interface/tcp"
	"github.com/hodis/lib/logger"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Address string `yaml:"address"`
	MaxConnect uint32 `yaml:"max-connect"`
	Timeout time.Duration `yaml:"timeout"`
}

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	//  当操作系统发出以下 4 个信号给程序时，传递信号给 sigCh
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	// 启一个协程来专门
	//监听并处理上述事件
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct {}{}
		}
	}()

	// 启动 pprof，用于性能测试分析
	// 如果不需要排查性能问题，也可以注释掉代码
	if config.Properties.ServerMode == config.SENTINEL {
		go func() {
			fmt.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 监听并处理关闭信号
	go func() {
		<-closeChan
		logger.Info("shutting down...")
		_ = listener.Close()
		_ = handler.Close()
	}()

	// 保证程序在异常终止时，也能及时关闭 监听和处理器
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	/*
	WaitGroup使用场景同名字的含义一样，当我们需要等待一组协程都执行完成以后，才能做后续的处理时，就可以考虑使用
	详解：https://segmentfault.com/a/1190000040653924
	 */
	var waitDone sync.WaitGroup

	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		//logger.Info("accept link")
		waitDone.Add(1)
		// 每收到一个客户端连接，就开一个协程来处理
		go func() {
			defer func() {
				waitDone.Done()
			}()
			//logger.Info("开始处理连接: ", conn.RemoteAddr().String())
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait()
}


