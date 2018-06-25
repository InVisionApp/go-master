// database initialization functionality
package mysql

import (
	"errors"
	"fmt"
	"time"

	"github.com/InVisionApp/go-logger"
	"github.com/InVisionApp/go-logger/shims/logrus"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const (
	DefaultDBName             = "gomaster"
	DefaultHeartbeatFrequency = time.Second * 5
	DefaultTableName          = "masterlock"
)

type MySQLBackend struct {
	db            *sqlx.DB
	log           log.Logger
	heartbeatFreq time.Duration

	*MySQLBackendConfig
}

type MySQLBackendConfig struct {
	// Supply a BaseDSN instead of credentials and host
	BaseDSN string

	// Supply User Password Host Port instead of a DSN
	User     string
	Password string
	Host     string
	Port     int //optional

	// Name of the DB to connect to.
	// If a name is supplied, a table will be created within that DB.
	// If a DB with the supplied name does not exist, and CreateDB is
	// set to true, it will be created.
	// If a DB name is not supplied, the default DB name will be used.
	// It will also attempt to create the DB if the CreateDB bool is
	// set to true.
	// DB creation is idempotent and can be left enabled if desired.
	DBName string

	// Optional: table name where the lock will exist (default: masterlock)
	TableName string

	// Optional: whether to attempt to create the DB
	CreateDB bool

	// optional params
	MaxWait    time.Duration
	MaxRetries int

	// Optional: Frequency of master heartbeat write
	HeartBeatFreq time.Duration

	// Optional: Logger for the mongo backend to use (default: new logrus shim will be created)
	Logger log.Logger

	// internal config
	driver string
}

func NewMySQLBackend(cfg *MySQLBackendConfig) *MySQLBackend {
	cfg.setDefaults()

	return &MySQLBackend{
		log:                cfg.Logger.WithFields(log.Fields{"pkg": "go-master.backend.mysql"}),
		heartbeatFreq:      cfg.HeartBeatFreq,
		MySQLBackendConfig: cfg,
	}
}

func (m *MySQLBackendConfig) setDefaults() {
	if m.Port == 0 {
		m.Port = 3306
	}

	if m.BaseDSN == "" {
		m.BaseDSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true", m.User, m.Password, m.Host, m.Port)
	}

	if m.MaxWait == 0 {
		m.MaxWait = time.Second * 5
	}

	if m.DBName == "" {
		m.DBName = DefaultDBName
	}

	if m.TableName == "" {
		m.TableName = DefaultTableName
	}

	if m.HeartBeatFreq == 0 {
		m.HeartBeatFreq = DefaultHeartbeatFrequency
	}

	if m.Logger == nil {
		m.Logger = logrus.New(nil)
	}

	m.driver = "mysql"
}

func (m *MySQLBackend) Connect() error {
	// attempt to connect to db server
	err := m.retryConnect()
	if err != nil {
		return err
	}

	// attempt to create DB enabled
	if m.CreateDB {
		if err := m.createDB(); err != nil {
			return err
		}
	}

	// use the appropriate DB
	if err := m.useDB(); err != nil {
		return err
	}

	if err := m.createTable(); err != nil {
		return fmt.Errorf("Unable to complete one or more migrations: %v", err.Error())
	}

	return nil
}

// Try to connect to a DB server
func (m *MySQLBackend) retryConnect() error {
	m.log.Debug("Attempting to connect to DB")

	var (
		errMessage string
		tries      int
	)

	for i := 0; i <= m.MaxRetries; i++ {
		tries++

		db, err := sqlx.Connect(m.driver, m.BaseDSN)
		if err != nil {
			//close the bad connection to prevent routine leak
			if db != nil {
				db.Close()
			}

			errMessage = fmt.Sprintf("Initial DB connection failed after %d attempts: %v", tries, err)
			m.log.Errorf("Unable to connect to DB: %v; Attempt %d/%d (retrying in %v)",
				err, tries, m.MaxRetries, m.MaxWait)

			time.Sleep(m.MaxWait)
			continue
		}

		m.db = db

		m.log.Debugf("Connected to DB %s:%d", m.Host, m.Port)
		return nil
	}

	return errors.New(errMessage)
}

func (m *MySQLBackend) createDB() error {
	m.log.Debug("Creating new lock DB if it does not exist")

	_, err := m.db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%v`", m.DBName))
	if err != nil {
		return fmt.Errorf("Unable to create initial lock DB: %v", err)
	}

	m.log.Debugf("Created new lock DB '%v' (or already existed)", m.DBName)

	return nil
}

func (m *MySQLBackend) useDB() error {
	if _, err := m.db.Exec(fmt.Sprintf("use `%v`", m.DBName)); err != nil {
		return fmt.Errorf("Unable to open db connection: %v", err)
	}

	m.log.Debugf("Using DB %s", m.DBName)

	return nil
}

func (m *MySQLBackend) createTable() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (`+
		`id INT NOT NULL PRIMARY KEY,`+
		`master_id CHAR(36) UNIQUE,`+
		`version VARCHAR(255),`+
		`started_at TIMESTAMP,`+
		`last_heartbeat TIMESTAMP`+
		`);`, m.TableName)

	m.log.Debug("Attempting to create lock table")

	_, err := m.db.DB.Exec(query)
	if err != nil {
		return fmt.Errorf("Unable to create lock table: %v", err)
	}

	m.log.Debugf("Created new lock table '%v' (or already existed)", m.TableName)

	return nil
}
