package mongo

import (
	"time"

	"crypto/tls"
	"fmt"
	"net"

	"strings"
	"sync"

	log "github.com/InVisionApp/go-logger"
	"github.com/InVisionApp/go-logger/shims/logrus"
	"github.com/globalsign/mgo"
	newrelic "github.com/newrelic/go-agent"
)

const (
	DefaultCollectionName     = "masterlock"
	DefaultHeartbeatFrequency = time.Second * 5

	MgoSessionRefreshFreq = time.Minute * 5
	DefaultPoolLimit      = 4
)

type MongoBackend struct {
	collName string
	lock     *SmartCollection
	indices  []*mgo.Index

	heartBeatFreq time.Duration

	cfg *MongoConnectConfig
	log log.Logger
}

type MongoBackendConfig struct {
	// Optional: What collection will the lock be stored in (default: "masterlock")
	CollectionName string

	// Required: Mongo connection config
	ConnectConfig *MongoConnectConfig

	// Optional: Frequency of master heartbeat write
	HeartBeatFreq time.Duration

	// Optional: Logger for the mongo backend to use (default: new logrus shim will be created)
	Logger log.Logger
}

type MongoConnectConfig struct {
	Hosts         []string
	Name          string
	ReplicaSet    string
	Source        string
	User          string
	Password      string
	Timeout       time.Duration
	UseSSL        bool
	PoolLimit     int
	MaxIdleTimeMS int
}

func New(cfg *MongoBackendConfig) *MongoBackend {
	setDefaults(cfg)

	return &MongoBackend{
		collName:      cfg.CollectionName,
		heartBeatFreq: cfg.HeartBeatFreq,
		log:           cfg.Logger,
		cfg:           cfg.ConnectConfig,
		indices: []*mgo.Index{
			{
				Name: "heartbeat_ttl",
				Key:  []string{"last_heartbeat"},
			},
		},
	}
}

func setDefaults(cfg *MongoBackendConfig) {
	if cfg.Logger == nil {
		cfg.Logger = logrus.New(nil)
	}

	if cfg.HeartBeatFreq == 0 {
		cfg.HeartBeatFreq = DefaultHeartbeatFrequency
	}

	if len(cfg.CollectionName) < 1 {
		cfg.CollectionName = DefaultCollectionName
	}

	if cfg.ConnectConfig.PoolLimit <= 0 {
		cfg.ConnectConfig.PoolLimit = DefaultPoolLimit
	}
}

func (m *MongoBackend) Connect() error {
	m.log.Infof("Connecting to DB: %q hosts: %v with timeout %d sec and pool size %v", m.cfg.Name, m.cfg.Hosts, m.cfg.Timeout, m.cfg.PoolLimit)
	m.log.Debugf("DB name: '%s'; replica set: '%s'; auth source: '%s'; user: '%s'; pass len: %d; use SSL: %v",
		m.cfg.Name, m.cfg.ReplicaSet, m.cfg.Source, m.cfg.User, len(m.cfg.Password), m.cfg.UseSSL)

	dialInfo := &mgo.DialInfo{
		Addrs:          m.cfg.Hosts,
		Database:       m.cfg.Name,
		ReplicaSetName: m.cfg.ReplicaSet,
		Source:         m.cfg.Source,
		Username:       m.cfg.User,
		Password:       m.cfg.Password,
		Timeout:        m.cfg.Timeout,
		PoolLimit:      m.cfg.PoolLimit,
		MaxIdleTimeMS:  m.cfg.MaxIdleTimeMS,
	}

	if m.cfg.UseSSL {
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), &tls.Config{})
			if conn != nil {
				m.log.Infof("Connection local address: %s, remote address: %s", conn.LocalAddr(), conn.RemoteAddr())
			}
			return conn, err
		}
	}

	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return fmt.Errorf("could not connect to MongoDB: %v", err)
	}

	// the lock db is special because data accuracy is more important here
	// strong mode will cause all reads and writes to go to the primary mongo node
	lc := session.Copy().DB(m.cfg.Name).C(m.collName)
	lc.Database.Session.SetMode(mgo.Strong, false)
	lc.Database.Session.SetSafe(&mgo.Safe{})
	m.lock = newSmartCollection(lc, MgoSessionRefreshFreq, m.log)
	m.lock.EnsureIndexes(m.indices)

	return nil
}

/*****************
 Smart Collection
*****************/

type SmartCollection struct {
	coll *mgo.Collection
	mu   RWLocker
	last time.Time
	freq time.Duration
	log  log.Logger
}

func newSmartCollection(c *mgo.Collection, freq time.Duration, log log.Logger) *SmartCollection {
	return &SmartCollection{
		coll: c,
		mu:   &sync.RWMutex{},
		last: time.Now(),
		freq: freq,
		log:  log,
	}
}

func (s *SmartCollection) Collection() *mgo.Collection {
	s.mu.RLock()
	elapsed := time.Since(s.last)
	s.mu.RUnlock()

	if elapsed > s.freq {
		s.mu.Lock()
		s.last = time.Now()
		s.mu.Unlock()

		// this is safe to do without a lock because it implements its own lock
		s.coll.Database.Session.Refresh()
	}

	return s.coll
}

func (s *SmartCollection) EnsureIndexes(idxs []*mgo.Index) error {
	for _, idx := range idxs {
		s.log.Infof("Ensuring index: %s", idx.Name)
		if err := s.UpsertIndex(idx); err != nil {
			return fmt.Errorf("could not ensure indexes on DB: %v", err)
		}
	}

	return nil
}

// Ensure new index. If index already exists with same options, remove it and add new one.
func (s *SmartCollection) UpsertIndex(idx *mgo.Index) error {
	if err := s.coll.EnsureIndex(*idx); err != nil {
		if strings.Contains(err.Error(), "already exists with different options") ||
			strings.Contains(err.Error(), "Trying to create an index with same name") {
			s.log.Warnf("index already exists with name '%s'. replacing...", idx.Name)

			//drop that one
			if err := s.coll.DropIndexName(idx.Name); err != nil {
				return fmt.Errorf("failed to remove old index: %v", err)
			}

			if err := s.coll.EnsureIndex(*idx); err != nil {
				return fmt.Errorf("failed to add new index: %v", err)
			}

			return nil
		}

		return fmt.Errorf("failed to ensure index: %v", err)
	}

	return nil
}

func (s *SmartCollection) StartMongoDatastoreSegment(txn newrelic.Transaction, op string, query map[string]interface{}) *newrelic.DatastoreSegment {
	return &newrelic.DatastoreSegment{
		StartTime:       newrelic.StartSegmentNow(txn),
		Product:         newrelic.DatastoreMongoDB,
		DatabaseName:    s.coll.Database.Name,
		Collection:      s.coll.Name,
		Operation:       op,
		QueryParameters: query,
	}
}

//go:generate counterfeiter -o ../../fakes/syncfakes/fake_rwLocker.go . RWLocker

type RWLocker interface {
	RLock()
	RUnlock()
	Lock()
	Unlock()
}
