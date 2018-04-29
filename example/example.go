package main

import (
	"time"
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/InVisionApp/go-logger"
	logshim "github.com/InVisionApp/go-logger/shims/logrus"

	"github.com/InVisionApp/go-master/backend/mongo"
	"github.com/InVisionApp/go-master"
)

var (
	logger log.Logger
)

func init() {
	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)
	logger = logshim.New(l)
}

func main() {
	mongoBackend := mongo.New(&mongo.MongoBackendConfig{
		CollectionName: "gomaster",
		ConnectConfig: &mongo.MongoConnectConfig{
			Hosts:      []string{"localhost"},
			Name:       "gomastertest",
			ReplicaSet: "",
			Source:     "",
			User:       "",
			Password:   "",
			Timeout:    time.Duration(1 * time.Second),
			UseSSL:     false,
		},
		Logger: logger,
		HeartBeatFreq: time.Second * 5,
	})

	if err := mongoBackend.Connect(); err != nil {
		logger.Errorf("Unable to connect to mongo: %v", err)
		os.Exit(1)
	}

	m := master.New(&master.MasterConfig{
		Version: "1.0",
		HeartBeatFrequency: time.Second * 5,
		StartHook: startHook,
		StopHook: stopHook,
		Err: make(chan error, 1000),
		Logger: log.NewSimple(),
		MasterLock: mongoBackend,
	})

	if err := m.Start(); err != nil {
		logger.Errorf("Unable to start go-master: %v", err)
		os.Exit(1)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func startHook() {
	fmt.Println("Have become master")
}

func stopHook() {
	fmt.Println("Lost master status")
}