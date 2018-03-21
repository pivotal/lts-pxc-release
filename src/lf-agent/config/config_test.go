package config_test

import (
	. "lf-agent/config"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {
	Describe("NewConfig", func() {
		It("returns a lf agent config", func() {
			configPath := "fixtures/config.yml"
			config, err := NewConfig(configPath)
			Expect(err).ToNot(HaveOccurred())
			Expect(config.HostAddress).To(Equal("10.0.0.1"))
			Expect(config.PeerAddress).To(Equal("10.0.0.2"))
			Expect(config.Datadir).To(Equal("/var/vcap/store/mysql/data"))
			Expect(config.EnableHeartbeats).To(Equal(true))
			Expect(config.LfStateDir).To(Equal("/path/to/lf-state-dir"))
			Expect(config.ReplicationAdminPassword).To(Equal("mysql-admin-password"))
			Expect(config.ReplicationAdminUser).To(Equal("mysql-admin-user"))
			Expect(config.ReplicationPassword).To(Equal("mysql-password"))
			Expect(config.ReplicationUser).To(Equal("mysql-user"))
			Expect(config.ReplicationWaitSeconds).To(Equal(10))
			Expect(config.ReplicationMode).To(Equal("semi-sync"))
			Expect(config.Port).To(Equal("8008"))
			Expect(config.HttpUsername).To(Equal("test-user"))
			Expect(config.HttpPassword).To(Equal("test-password"))
			Expect(config.StreamingBackupPort).To(Equal("8081"))
			Expect(config.StreamingBackupHttpUsername).To(Equal("streaming-backup-username"))
			Expect(config.StreamingBackupHttpPassword).To(Equal("streaming-backup-password"))
			Expect(config.SSLCACertPath).To(Equal("/path/to/server.ca"))
			Expect(config.SSLCommonName).To(Equal("lf-agent-common"))
			Expect(config.SSLServerCertPath).To(Equal("/path/to/server.cert"))
			Expect(config.SSLServerKeyPath).To(Equal("/path/to/server.key"))
			Expect(config.SSLClientCertPath).To(Equal("/path/to/client.cert"))
			Expect(config.SSLClientKeyPath).To(Equal("/path/to/client.key"))
			Expect(config.StreamingBackupCACertPath).To(Equal("/path/to/streaming-backup-ca.cert"))
			Expect(config.StreamingBackupSSLCommonName).To(Equal("streaming-backup-common-name"))
			Expect(config.MySQLCACertPath).To(Equal("/path/to/mysql/mysql.ca"))
		})

		It("returns an error when the path does not exist", func() {
			configPath := "some/nonexistent/path"
			_, err := NewConfig(configPath)
			Expect(err).To(MatchError(ContainSubstring("unable to find config file at path: some/nonexistent/path")))
		})

		It("returns an error when the config file is invalid yaml", func() {
			configPath := "fixtures/invalid-config.yml"
			_, err := NewConfig(configPath)
			Expect(err).To(MatchError(ContainSubstring("unable to read config file:")))
		})
	})
})
