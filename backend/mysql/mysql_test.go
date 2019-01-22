package mysql

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MySQL-unit", func() {
	Context("New", func() {
		It("will set defaults and instantiate appropriately", func() {
			cfg := &MySQLBackendConfig{}
			b := NewMySQLBackend(cfg)

			Expect(cfg.Port).To(Equal(3306))
			Expect(cfg.BaseDSN).ToNot(BeEmpty())
			Expect(cfg.MaxWait).To(Equal(DefaultMaxWait))
			Expect(cfg.DBName).To(Equal(DefaultDBName))
			Expect(cfg.TableName).To(Equal(DefaultTableName))
			Expect(cfg.HeartBeatFreq).To(Equal(DefaultHeartbeatFrequency))
			Expect(cfg.Logger).ToNot(BeNil())
			Expect(cfg.MaxOpenConnections).To(Equal(DefaultMaxOpenConnections))
			Expect(cfg.driver).To(Equal("mysql"))

			Expect(b.log).ToNot(BeNil())
			Expect(b.heartbeatFreq).To(Equal(cfg.HeartBeatFreq))
			Expect(b.MySQLBackendConfig).To(Equal(cfg))
		})
	})
})
