// +build integration

package mysql

import (
	"fmt"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

const (
	TestDBUser = "root"
	TestDBPass = "invision"
	TestDBHost = "localhost"
	TestDBName = "gomaster_test"
)

var _ = Describe("MySQL-unit", func() {
	Describe("Connect", func() {
		var (
			cfg *MySQLBackendConfig
		)

		BeforeEach(func() {
			var generateErr error

			cfg, generateErr = generateConfig()
			Expect(generateErr).ToNot(HaveOccurred())

			db, err := sqlx.Open("mysql", cfg.BaseDSN)
			Expect(err).ToNot(HaveOccurred())

			_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %v", cfg.DBName))
			Expect(err).ToNot(HaveOccurred(), "db removal before test should not error")
		})

		AfterEach(func() {
			db, err := sqlx.Open("mysql", cfg.BaseDSN)
			Expect(err).ToNot(HaveOccurred())

			_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %v", cfg.DBName))
			Expect(err).ToNot(HaveOccurred(), "db removal after test should not error")
		})

		Context("happy path", func() {
			It("connects successfully -> creates db -> creates table(s)", func() {
				b := NewMySQLBackend(cfg)
				err := b.Connect()

				Expect(err).ToNot(HaveOccurred(), "should be able to connect")

				// Assert DB was created
				db, err := sqlx.Open("mysql", b.BaseDSN)
				Expect(err).ToNot(HaveOccurred())
				Expect(db).ToNot(BeNil())

				dbs := []string{}
				db.Select(&dbs, "SHOW DATABASES")

				Expect(dbs).To(ContainElement(cfg.DBName))

				// Assert table was created
				_, err = db.Exec(fmt.Sprintf("use %v", cfg.DBName))
				Expect(err).ToNot(HaveOccurred())

				tables := []string{}
				db.Select(&tables, "SHOW TABLES")

				Expect(tables).To(ContainElement(cfg.TableName))
			})
		})

		Context("when initial connection fails", func() {
			It("should error", func() {
				// Copy cfg (so AfterEach still works)
				cfgCopy := *cfg
				cfgCopy.BaseDSN = "" // reset dsn so it gets set during NewMySQLBackend
				cfgCopy.Port = 4417

				b := NewMySQLBackend(&cfgCopy)
				err := b.Connect()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("Initial DB connection failed"))
			})
		})

		Context("when db creation is set to false", func() {
			It("should not create a database", func() {
				cfg.CreateDB = false

				b := NewMySQLBackend(cfg)
				err := b.Connect()

				// Because we tell go-master to NOT create a DB, the attempt to
				// _use_ the database should fail (as the DB is not there)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("Unknown database"))
			})
		})

		Context("when db creation fails", func() {
			It("should return an error", func() {
				// create copy to avoid AfterEach failures
				cfgCopy := *cfg
				cfgCopy.BaseDSN = ""
				cfgCopy.DBName = "gomaster-boop-`"

				b := NewMySQLBackend(&cfgCopy)
				err := b.Connect()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unable to create initial lock DB"))
			})
		})

		Context("when table creation fails", func() {
			It("should return an error", func() {
				// create copy to avoid AfterEach failures
				cfgCopy := *cfg
				cfgCopy.BaseDSN = ""
				cfgCopy.TableName = "bad-table-name`"

				b := NewMySQLBackend(&cfgCopy)
				err := b.Connect()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unable to create lock table"))
			})
		})
	})
})

func generateConfig() (*MySQLBackendConfig, error) {
	cfg := &MySQLBackendConfig{
		User:     os.Getenv("MYSQL_USER"),
		Password: os.Getenv("MYSQL_PASSWORD"),
		Host:     os.Getenv("MYSQL_HOST"),
		DBName:   TestDBName,
		CreateDB: true,
		MaxWait:  time.Second * 1,
	}

	if cfg.User == "" {
		cfg.User = TestDBUser
	}

	if cfg.Password == "" {
		cfg.Password = TestDBPass
	}

	if cfg.Host == "" {
		cfg.Host = TestDBHost
	}

	cfg.Port, _ = strconv.Atoi(os.Getenv("MYSQL_PORT"))

	// Set BaseDN which we use in tests
	cfg.setDefaults()

	return cfg, nil
}
