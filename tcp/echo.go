package tcp

import (
	"bufio"
	"context"
	"gocache/lib/logger"
	"gocache/lib/sync/atomic"
	"gocache/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

type EchoClient struct {
	Conn net.Conn
	Waiting wait.Wait
}

func (e *EchoClient) Close() error {
	e.Waiting.WaitWithTimeout(10 * time.Second)
	_ = e.Conn.Close()
	return nil
}

type EchoHandler struct {
	activeConn sync.Map
	closing atomic.Boolean
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if handler.closing.Get() {
		_ = conn.Close()
		return
	}

	client := &EchoClient{
		Conn: conn,
	}

	handler.activeConn.Store(client, struct{}{})
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("Connection closing")
				handler.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		// critical part: using waitgroup to ensure that close() not interrupting write()
		client.Waiting.Add(1)
		b := []byte(msg)
		conn.Write(b)
		client.Waiting.Done()
	}
}

func (handler *EchoHandler) Close() error {
	logger.Info("handler shutting down")
	handler.closing.Set(true)
	handler.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*EchoClient)
		client.Conn.Close()
		return true
	})	
	return nil
}