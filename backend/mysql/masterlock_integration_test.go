//go:build integration
// +build integration

package mysql

import (
	"context"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/InVisionApp/go-master"
	"github.com/InVisionApp/go-master/backend"
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
		It("happy path - no existing master", func() {
			masterInfo := &backend.MasterInfo{
				MasterID:      "testMasterID",
				Version:       "testVersion",
				StartedAt:     time.Now().Add(100 * time.Second),
				LastHeartbeat: time.Now(),
			}

			err := be.Lock(context.TODO(), masterInfo)
			Expect(err).ToNot(HaveOccurred())

			// Verify that masterInfo in DB server is the same as ours
			dbMasterInfo, hasMaster, err := be.getMasterInfo()
			Expect(err).ToNot(HaveOccurred())
			Expect(hasMaster).To(BeTrue())
			Expect(dbMasterInfo.MasterID).To(Equal(masterInfo.MasterID))
		})

		It("happy path - take over existing, expired master", func() {
			// Difficult to test
		})

		Context("when active master exists", func() {
			It("should return active master error", func() {
				// Start up go-master
				gom := master.New(&master.MasterConfig{
					MasterLock:         be,
					HeartBeatFrequency: 50 * time.Millisecond,
				})

				err := gom.Start()
				Expect(err).ToNot(HaveOccurred())

				// Give some time for master to start
				time.Sleep(100 * time.Millisecond)

				// should have a masterInfo
				masterInfo, hasMaster, err := be.getMasterInfo()
				Expect(err).ToNot(HaveOccurred())
				Expect(hasMaster).To(BeTrue())
				Expect(masterInfo).ToNot(BeNil())

				// Performing another lock should cause us to get a lock error
				err = be.Lock(context.TODO(), masterInfo.toMasterInfo())
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("found active master"))
			})
		})

	})

	Describe("UnLock", func() {
		It("happy path", func() {
			// Start up go-master
			gom := master.New(&master.MasterConfig{
				MasterLock:         be,
				HeartBeatFrequency: 50 * time.Millisecond,
			})

			err := gom.Start()
			Expect(err).ToNot(HaveOccurred())

			// Give some time for master to start
			time.Sleep(100 * time.Millisecond)

			// should have a masterInfo
			masterInfo, hasMaster, err := be.getMasterInfo()
			Expect(err).ToNot(HaveOccurred())
			Expect(hasMaster).To(BeTrue())
			Expect(masterInfo).ToNot(BeNil())

			err = be.UnLock(context.TODO(), masterInfo.MasterID)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when exec fails", func() {
			It("should return error", func() {
				// Closing the DB connection will cause the unlock to error
				err := be.db.Close()
				Expect(err).ToNot(HaveOccurred(), "closing db connection should not error")

				err = be.UnLock(context.TODO(), "foo")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to release master lock"))
			})
		})
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

				masterInfo, err := be.Status(context.TODO())
				Expect(err).ToNot(HaveOccurred())
				Expect(masterInfo).ToNot(BeNil())
				Expect(masterInfo.MasterID).ToNot(BeEmpty())
			})
		})

		Context("when there is no master", func() {
			It("return error", func() {
				masterInfo, err := be.Status(context.TODO())
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
