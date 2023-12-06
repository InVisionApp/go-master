package main

import (
	"context"
	"os"
	"sync"
	"time"

	log "github.com/InVisionApp/go-logger"
	logshim "github.com/InVisionApp/go-logger/shims/logrus"
	"github.com/sirupsen/logrus"

	"github.com/InVisionApp/go-master"
	"github.com/InVisionApp/go-master/backend"
	"github.com/InVisionApp/go-master/backend/mongo"
	"github.com/InVisionApp/go-master/backend/mysql"
)

var (
	logger log.Logger
)

func init() {
	l := logrus.New()
	l.SetLevel(logrus.InfoLevel)
	logger = logshim.New(l)
}

func main() {
	//masterLock := MongDBBackend()
	masterLock := MySQLBackend()

	m := master.New(&master.MasterConfig{
		StartHook:  startHook,
		StopHook:   stopHook,
		MasterLock: masterLock,
		Logger:     logger,
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

func MongDBBackend() backend.MasterLock {
	mongoBackend := mongo.New(&mongo.MongoBackendConfig{
		CollectionName: "gomaster",
		ConnectConfig: &mongo.MongoConnectConfig{
			Hosts:     []string{"localhost"},
			Name:      "gomastertest",
			Timeout:   time.Duration(1 * time.Second),
			UseSSL:    false,
			PoolLimit: 4,
		},
		Logger: logger,
	})

	ctx := context.TODO()

	if err := mongoBackend.Connect(ctx); err != nil {
		logger.Errorf("Unable to connect to mongo: %v", err)
		os.Exit(1)
	}

	return mongoBackend
}

func MySQLBackend() backend.MasterLock {
	mysqlBackend := mysql.NewMySQLBackend(&mysql.MySQLBackendConfig{
		User:               "foo",
		Password:           "bar",
		Host:               "localhost",
		Port:               3306,
		DBName:             "gomastertest",
		CreateDB:           true,
		Logger:             logger,
		MaxOpenConnections: 5,
	})

	if err := mysqlBackend.Connect(); err != nil {
		logger.Errorf("Unable to connect to MySQL: %v", err)
		os.Exit(1)
	}

	return mysqlBackend
}

func startHook(ctx context.Context) {
	logger.Info("Became master")
}

func stopHook(ctx context.Context) {
	logger.Info("Lost master status")
}
