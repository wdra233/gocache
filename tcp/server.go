package tcp

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"gocache/interface/tcp"
	"gocache/lib/logger"
)

type Config struct {
	Address string
}

func ListenAndServeWithSignal(
	cfg *Config,
	handler tcp.Handler,
) error {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info("start listening")

	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		signal := <-sigChan
		switch signal {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {

	go func() {
		<-closeChan
		logger.Info("shutting down server....")
		_ = listener.Close()
		_ = handler.Close()
	}()

	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		logger.Info("accepted connection")
		waitDone.Add(1)
		go func() {
			defer func() {waitDone.Done()}()
			handler.Handle(ctx, conn)
		}()

	}
	waitDone.Wait()
}
