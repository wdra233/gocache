package handler

import (
	"context"
	"gocache/cluster"
	"gocache/config"
	"gocache/database"
	databaseface "gocache/interface/database"
	"gocache/lib/logger"
	"gocache/lib/sync/atomic"
	"gocache/resp/connection"
	"gocache/resp/parser"
	"gocache/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)


type RespHandler struct {
	activeConn sync.Map
	db databaseface.Database
	closing atomic.Boolean
}

func MakeHandler() *RespHandler {
	var db databaseface.Database
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		db = cluster.MakeClusterDatabase()
	} else {
		db = database.NewDatabase()
	}
	return &RespHandler{
		db: db,
	}
}
func (r *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	r.db.AfterClientClose(client)
	r.activeConn.Delete(client)
}

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if r.closing.Get() {
		_ = conn.Close()
	}
	client := connection.NewConn(conn)
	r.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn)
	for payload := range ch {
		// error
		if payload.Err != nil {
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || 
			strings.Contains(payload.Err.Error(), "use of closed network connection") {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol error
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		// exec
		if payload.Data == nil {
			continue
		}
		reply, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := r.db.Exec(client, reply.Args)
		if result != nil {
			client.Write(result.ToBytes())
		} else {
			client.Write(unknownErrReplyBytes)
		}
	}
}

func (r *RespHandler) Close() error {
	logger.Info("handler shutting down")
	r.closing.Set(true)
	r.activeConn.Range(
		func(key, value any) bool {
			client := key.(*connection.Connection)
			_ = client.Close()
			return true
		},
	)
	r.db.Close()
	return nil
}

