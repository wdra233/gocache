package main

import (
	"fmt"
	"gocache/config"
	"gocache/lib/logger"
	"gocache/resp/handler"
	"gocache/tcp"
	"os"
)

const configFile string = "redis.conf"

var defaultProperties = &config.ServerProperties{
	Bind: "0.0.0.0",
	Port: 6379,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	logger.Setup(&logger.Settings{
		Path: "logs",
		Name: "gocache",
		Ext: "log",
		TimeFormat: "01-02-2006",

	})

	if fileExists(configFile) {
		config.SetupConfig(configFile)
	} else {
		config.Properties = defaultProperties
	}

	err := tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		},
		handler.MakeHandler(),
	)
	if err != nil {
		logger.Error(err)
	}

}