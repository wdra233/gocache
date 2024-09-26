package aof

import (
	"gocache/config"
	"gocache/interface/database"
	"gocache/lib/logger"
	"gocache/lib/utils"
	"gocache/resp/connection"
	"gocache/resp/parser"
	"gocache/resp/reply"
	"io"
	"os"
	"strconv"
)

const aofBufferSize = 1 << 16
type CmdLine = [][]byte

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

type AofHandler struct {
	database database.Database
	aofChan chan *payload
	aofFile *os.File
	aofFilename string
	currentDB int
}

//NewAofHandler

func NewAofHandler(database database.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.database = database

	handler.LoadAof()
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile

	handler.aofChan = make(chan *payload, aofBufferSize)
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// Add payload(set k v) -> aofChan
func(handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}
}

// handleAof payload(set k v) <- aofChan
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		if handler.currentDB != p.dbIndex {
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
			}
			handler.currentDB = p.dbIndex
		}
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
		}
	}
}

// LoadAof
func(handler *AofHandler) LoadAof() {
	file, err := os.Open(handler.aofFilename)
	fakeConn := &connection.Connection{}
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()

	ch := parser.ParseStream(file)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(p.Err)
			continue
		}

		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("need multi bulk")
			continue
		}

		rep := handler.database.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(rep) {
			logger.Error("exec err", rep.ToBytes())
		}

	}

}