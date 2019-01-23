// +build integration

package mysql

import (
	"fmt"
	"time"

	"github.com/InVisionApp/go-master"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("masterlock-integration", func() {
	var (
		cfg *MySQLBackendConfig
		be  *MySQLBackend
	)

	// Tear down _potentially_ pre-existing DB && start-up fresh go-master after each `It` block
	JustBeforeEach(func() {
		var generateErr error

		cfg, generateErr = generateConfig()
		Expect(generateErr).ToNot(HaveOccurred())

		db, err := sqlx.Open("mysql", cfg.BaseDSN)
		Expect(err).ToNot(HaveOccurred())

		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %v", cfg.DBName))
		Expect(err).ToNot(HaveOccurred(), "db removal before test should not error")

		// Setup MySQL backend
		be = NewMySQLBackend(cfg)
		connectErr := be.Connect()

		Expect(connectErr).ToNot(HaveOccurred())
	})

	// Tear down DB after each `It` block
	JustAfterEach(func() {
		db, err := sqlx.Open("mysql", cfg.BaseDSN)
		Expect(err).ToNot(HaveOccurred())

		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %v", cfg.DBName))
		Expect(err).ToNot(HaveOccurred(), "db removal after test should not error")
	})

	Describe("Lock", func() {

	})

	Describe("UnLock", func() {

	})

	Describe("WriteHeartbeat", func() {

	})

	Describe("Status", func() {
		Context("happy path", func() {
			It("should return master info and not error", func() {
				// Start go-master
				gom := master.New(&master.MasterConfig{
					MasterLock:         be,
					HeartBeatFrequency: 50 * time.Millisecond,
				})

				err := gom.Start()
				Expect(err).ToNot(HaveOccurred())

				// Give some time for master to start
				time.Sleep(100 * time.Millisecond)

				masterInfo, err := be.Status()
				Expect(err).ToNot(HaveOccurred())
				Expect(masterInfo).ToNot(BeNil())
				Expect(masterInfo.MasterID).ToNot(BeEmpty())
			})
		})

		Context("when there is no master", func() {
			It("return error", func() {
				masterInfo, err := be.Status()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no master currently"))
				Expect(masterInfo).To(BeNil())
			})
		})

		Context("when fetching master info fails", func() {
			It("should return an error", func() {
				// Hard to test this case
			})
		})
	})
})
