package main

import (
	"fmt"
	"github.com/hodis/config"
	"github.com/hodis/lib/logger"
	"github.com/hodis/redis/server"
	"github.com/hodis/tcp"
	"os"
)

var banner = `
   ______          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "hodis.aof",
	MaxClients:     1000,
}


func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	print(banner)
	logger.Setup(&logger.Settings{
		Path: "logs",
		Name: "hodis",
		Ext: "log",
		TimeFormat: "2022-10-29",
	})
	var configFilename = ""
	// 如果用户在命令行指定了配置文件，就优先读命令行
	if len(os.Args) == 2 {
		configFilename = os.Args[1]
	} else if fileExists("hodis.conf") {
		// 如果当前目录下有配置文件，就读配置文件
		configFilename = "hodis.conf"
	} else if len(os.Args) < 2 {
		// 如果命令行没指定，当前目录下没有配置文件，就读环境变量
		configFilename = os.Getenv("CONFIG")
	}


	if configFilename == "" {
		// 要是什么都没有，就用默认配置
		config.Properties = defaultProperties
	} else {
		config.SetupConfig(configFilename)

	}

	tcpServerConfig := &tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}

	err := tcp.ListenAndServeWithSignal(tcpServerConfig, server.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}

