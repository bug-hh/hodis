package server

import (
	"github.com/hodis/config"
	database2 "github.com/hodis/database"
	"github.com/hodis/interface/database"
	"github.com/hodis/lib/sync/atomic"
	"sync"
)

// Handler implements tcp.Handler and serves as a redis server
type Handler struct {
	activeConn sync.Map
	db database.DB
	closing atomic.Boolean
}


func MakeHandler() *Handler {
	var db database.DB

	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		// 开启集群模式
	} else {
		// 开启单机模式
		database2.NewStandaloneServer()
	}
	return &Handler{
		db: db,
	}
}
