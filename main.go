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
	AppendFilename: "",
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

	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
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