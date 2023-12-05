package mongo

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/InVisionApp/go-logger"
	"github.com/InVisionApp/go-logger/shims/logrus"
	newrelic "github.com/newrelic/go-agent"
	mgo "github.com/qiniu/qmgo"
	mgooptions "github.com/qiniu/qmgo/options"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	DefaultCollectionName     = "masterlock"
	DefaultHeartbeatFrequency = time.Second * 5

	DefaultPoolLimit = 4
)

type MongoBackend struct {
	collName string
	lock     *SmartCollection
	indices  []mgooptions.IndexModel

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
	URL           string
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
		indices: []mgooptions.IndexModel{
			{
				Key:          []string{"last_heartbeat"},
				IndexOptions: options.Index().SetName("heartbeat_ttl"),
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

func (m *MongoBackend) Connect(ctx context.Context) error {
	m.log.Infof("Connecting to DB: %q hosts: %v with timeout %d sec and pool size %v", m.cfg.Name, m.cfg.Hosts, m.cfg.Timeout, m.cfg.PoolLimit)
	m.log.Debugf("DB name: '%s'; replica set: '%s'; auth source: '%s'; user: '%s'; pass len: %d; use SSL: %v",
		m.cfg.Name, m.cfg.ReplicaSet, m.cfg.Source, m.cfg.User, len(m.cfg.Password), m.cfg.UseSSL)

	mongoClient, err := m.connectMongoClient(ctx)
	if err != nil {
		return err
	}

	db := mongoClient.Database(m.cfg.Name)
	lc := db.Collection(m.collName)

	m.lock = newSmartCollection(mongoClient, db, lc, m.log)
	m.lock.coll.CreateIndexes(ctx, m.indices)

	return nil
}

func (m *MongoBackend) Disconnect(ctx context.Context) error {
	return m.lock.client.Close(ctx)
}

func (m *MongoBackend) connectMongoClient(ctx context.Context) (*mgo.Client, error) {
	clientConfig := &mgo.Config{
		Uri: m.cfg.URL,
	}

	if clientConfig.Uri == "" {
		// NOTE: A base URI is required by qmgo, and the rest of the options
		// will be applied by it later into this URI (but won't override any param
		// that might already exist in the URI).
		clientConfig.Uri = "mongodb://" + strings.Join(m.cfg.Hosts, ",")
	}

	client, err := mgo.NewClient(ctx, clientConfig, m.getMongoClientOptions())
	if err != nil {
		return nil, fmt.Errorf("Could not connect to MongoDB: %v", err)
	}

	return client, nil
}

func (m *MongoBackend) getMongoClientOptions() mgooptions.ClientOptions {
	clientOptions := options.Client()
	clientOptions.SetHosts(m.cfg.Hosts)
	clientOptions.SetReplicaSet(m.cfg.ReplicaSet)
	clientOptions.SetReadPreference(readpref.Primary())
	clientOptions.SetReadConcern(readconcern.Majority())
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	clientOptions.SetConnectTimeout(m.cfg.Timeout)
	clientOptions.SetMaxPoolSize(uint64(m.cfg.PoolLimit))
	clientOptions.SetMaxConnIdleTime(time.Duration(m.cfg.MaxIdleTimeMS) * time.Millisecond)
	clientOptions.SetAuth(options.Credential{
		Username:   m.cfg.User,
		Password:   m.cfg.Password,
		AuthSource: m.cfg.Source,
	})

	if m.cfg.UseSSL {
		clientOptions.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	return mgooptions.ClientOptions{
		ClientOptions: clientOptions,
	}
}

/*****************
 Smart Collection
*****************/

type SmartCollection struct {
	client *mgo.Client
	db     *mgo.Database
	coll   *mgo.Collection
	mu     RWLocker
	last   time.Time
	freq   time.Duration
	log    log.Logger
}

func newSmartCollection(client *mgo.Client, db *mgo.Database, c *mgo.Collection, log log.Logger) *SmartCollection {
	return &SmartCollection{
		client: client,
		db:     db,
		coll:   c,
		mu:     &sync.RWMutex{},
		last:   time.Now(),
		log:    log,
	}
}

func (s *SmartCollection) Collection() *mgo.Collection {
	// NOTE: The way connections are managed have changed completely
	// and there is no more session neither the need to refresh the session,
	// therefore we shouldn't need this session refreshing anymore here.
	// Keeping this around anyway as commented code just for future reference in case we
	// notice something wrong with this change.

	// s.mu.RLock()
	// elapsed := time.Since(s.last)
	// s.mu.RUnlock()
	//
	// if elapsed > s.freq {
	// 	s.mu.Lock()
	// 	s.last = time.Now()
	// 	s.mu.Unlock()
	//
	// 	// this is safe to do without a lock because it implements its own lock
	// 	s.coll.Database.Session.Refresh()
	// }

	return s.coll
}

func (s *SmartCollection) EnsureIndexes(ctx context.Context, idxs []*mgooptions.IndexModel) error {
	for _, idx := range idxs {
		s.log.Infof("Ensuring index: %s", idx.Name)
		if err := s.UpsertIndex(ctx, idx); err != nil {
			return fmt.Errorf("could not ensure indexes on DB: %v", err)
		}
	}

	return nil
}

// Ensure new index. If index already exists with same options, remove it and add new one.
func (s *SmartCollection) UpsertIndex(ctx context.Context, idx *mgooptions.IndexModel) error {
	if idx.Name == nil {
		return fmt.Errorf("index is missing a name: %+v", idx)

	}

	if err := s.coll.CreateOneIndex(ctx, *idx); err != nil {
		if strings.Contains(err.Error(), "already exists with different options") ||
			strings.Contains(err.Error(), "Trying to create an index with same name") {
			s.log.Warnf("index already exists with name '%s'. replacing...", idx.Name)

			//drop that one
			if err := s.coll.DropIndex(ctx, []string{*idx.Name}); err != nil {
				return fmt.Errorf("failed to remove old index: %v", err)
			}

			if err := s.coll.CreateOneIndex(ctx, *idx); err != nil {
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
		DatabaseName:    s.db.GetDatabaseName(),
		Collection:      s.coll.GetCollectionName(),
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
