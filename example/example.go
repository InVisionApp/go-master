package main

import (
	"os"
	"sync"
	"time"

	"github.com/InVisionApp/go-logger"
	logshim "github.com/InVisionApp/go-logger/shims/logrus"
	"github.com/sirupsen/logrus"

	"github.com/InVisionApp/go-master"
	"github.com/InVisionApp/go-master/backend/mongo"
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
	})

	if err := mongoBackend.Connect(); err != nil {
		logger.Errorf("Unable to connect to mongo: %v", err)
		os.Exit(1)
	}

	m := master.New(&master.MasterConfig{
		StartHook:  startHook,
		StopHook:   stopHook,
		MasterLock: mongoBackend,
	})

	if err := m.Start(); err != nil {
		logger.Errorf("Unable to start go-master: %v", err)
		os.Exit(1)
	}

	logger.Infof("go-master instance started w/ id: %v", m.ID())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func startHook() {
	logger.Info("Became master")
}

func stopHook() {
	logger.Info("Lost master status")
}
