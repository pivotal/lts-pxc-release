package database_test

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"lf-agent/agent"
	"lf-agent/config"
	. "lf-agent/database"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("Database", func() {

	var (
		mock   sqlmock.Sqlmock
		db     *Database
		driver *sql.DB

		cfg config.Config
	)

	BeforeEach(func() {
		cfg = config.Config{
			HostAddress: "10.0.0.1",
		}
	})

	JustBeforeEach(func() {
		var err error
		driver, mock, err = sqlmock.New()
		Expect(err).NotTo(HaveOccurred())

		db = NewDatabase(
			driver,
			cfg,
		)
	})

	AfterEach(func() {
		Expect(mock.ExpectationsWereMet()).To(Succeed())
	})

	Describe("Status Information", func() {

		Context("Read Only", func() {
			It("returns 'true' if the instance is read-only", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow(""))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				status, _ := db.Status()
				Expect(status.ReadOnly).To(BeTrue())
			})

			It("returns 'false' if the instance is writable", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow(""))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("0"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				status, _ := db.Status()
				Expect(status.ReadOnly).To(BeFalse())
			})

			It("returns an error when checking for read-only fails ", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow(""))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnError(errors.New("some-error"))

				_, err := db.Status()
				Expect(err).To(MatchError("could not retrieve db-status: some-error"))
			})

		})

		Context("Replication Configured", func() {
			It("returns 'true' if the replication connection status is healthy", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow(""))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("0"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				status, _ := db.Status()
				Expect(status.ReplicationConfigured).To(BeTrue())
			})

			It("returns 'false' if the replication connection status is not configured", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow(""))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("0"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				status, _ := db.Status()
				Expect(status.ReplicationConfigured).To(BeFalse())
			})

			It("returns an error when checking for replication configured fails", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow(""))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("0"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnError(errors.New("some-error"))

				_, err := db.Status()
				Expect(err).To(MatchError("could not retrieve db-status: some-error"))
			})
		})

		Context("Replication Mode", func() {
			It("returns 'async' if the instance not configured with semi-sync plugins", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow("f01172bb-d9fc-11e7-af9d-2a2800d3b10f:1-3"))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				status, err := db.Status()
				Expect(err).NotTo(HaveOccurred())
				Expect(status.ReplicationMode).To(Equal(agent.ReplicationAsync))
			})

			It("returns 'semi-sync' if the leader if configured with semi-sync", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow("f01172bb-d9fc-11e7-af9d-2a2800d3b10f:1-3"))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Rpl_semi_sync_master_status'`).
					WillReturnRows(sqlmock.NewRows([]string{"Value"}).
						AddRow("ON"))

				mock.ExpectQuery(`SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Rpl_semi_sync_slave_status'`).
					WillReturnRows(sqlmock.NewRows([]string{"Value"}).
						AddRow("OFF"))

				status, err := db.Status()
				Expect(err).NotTo(HaveOccurred())
				Expect(status.ReplicationMode).To(Equal(agent.ReplicationSemisync))
			})

			It("returns 'semi-sync' if the follower if configured with semi-sync", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow("f01172bb-d9fc-11e7-af9d-2a2800d3b10f:1-3"))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Rpl_semi_sync_master_status'`).
					WillReturnRows(sqlmock.NewRows([]string{"Value"}).
						AddRow("OFF"))

				mock.ExpectQuery(`SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Rpl_semi_sync_slave_status'`).
					WillReturnRows(sqlmock.NewRows([]string{"Value"}).
						AddRow("ON"))

				status, err := db.Status()
				Expect(err).NotTo(HaveOccurred())
				Expect(status.ReplicationMode).To(Equal(agent.ReplicationSemisync))
			})

			It("returns an error when checking for the replication mode fails", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow("f01172bb-d9fc-11e7-af9d-2a2800d3b10f:1-3"))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Rpl_semi_sync_master_status'`).
					WillReturnError(errors.New("some-error"))

				_, err := db.Status()
				Expect(err).To(MatchError("could not retrieve db-status: some-error"))
			})
		})

		Context("GTID Executed", func() {
			It("returns the correct GTID of the instance", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.gtid_executed"}).
						AddRow("f01172bb-d9fc-11e7-af9d-2a2800d3b10f:1-3"))

				mock.ExpectQuery(`SELECT @@global.super_read_only`).
					WillReturnRows(sqlmock.NewRows([]string{"@@global.super_read_only"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("1"))

				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT(*) = 1"}).
						AddRow("0"))

				status, _ := db.Status()
				Expect(status.GtidExecuted).To(Equal("f01172bb-d9fc-11e7-af9d-2a2800d3b10f:1-3"))
			})
			It("returns an error when checking for gtid fails ", func() {
				mock.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnError(errors.New("some-error"))

				_, err := db.Status()
				Expect(err).To(MatchError("could not retrieve db-status: some-error"))
			})
		})
	})

	Describe("ApplyGTIDPurged", func() {

		var tempDir string

		BeforeEach(func() {
			tempDir, err := ioutil.TempDir("", "mysql-test-")
			Expect(err).NotTo(HaveOccurred())

			cfg.Datadir = tempDir
		})

		AfterEach(func() {
			os.RemoveAll(tempDir)
		})

		It("sets a gtid_purged property onto the db", func() {
			binlogInfoFile := filepath.Join(db.Cfg.Datadir, "xtrabackup_binlog_info")
			err := ioutil.WriteFile(binlogInfoFile, []byte("mysql-bin.000002\t1855\t33209bc5-c64a-11e7-b8ec-ee6f44cbb206:1-5\n"), 0600)
			Expect(err).NotTo(HaveOccurred())

			mock.ExpectExec(`RESET MASTER`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`SET GLOBAL gtid_purged = \?`).
				WithArgs("33209bc5-c64a-11e7-b8ec-ee6f44cbb206:1-5").
				WillReturnResult(sqlmock.NewResult(1, 1))

			err = db.ApplyGTIDPurged()
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns a error when the binlog info file is not in expected format", func() {
			binlogInfoFile := filepath.Join(db.Cfg.Datadir, "xtrabackup_binlog_info")
			err := ioutil.WriteFile(binlogInfoFile, []byte("mysql-bin.000002\t1855"), 0600)
			Expect(err).NotTo(HaveOccurred())

			err = db.ApplyGTIDPurged()
			Expect(err).To(MatchError("gtid_purged not found in xtrabackup_binlog_info"))
		})

		It("returns a error when resetting fails", func() {
			binlogInfoFile := filepath.Join(db.Cfg.Datadir, "xtrabackup_binlog_info")
			err := ioutil.WriteFile(binlogInfoFile, []byte("mysql-bin.000002\t1855\t33209bc5-c64a-11e7-b8ec-ee6f44cbb206:1-5\n"), 0600)
			Expect(err).NotTo(HaveOccurred())

			mock.ExpectExec(`RESET MASTER`).WillReturnError(errors.New("some-db-error"))

			err = db.ApplyGTIDPurged()
			Expect(err).To(MatchError("some-db-error"))
		})

		It("returns a error when setting global gtid_purged fails", func() {
			binlogInfoFile := filepath.Join(db.Cfg.Datadir, "xtrabackup_binlog_info")
			err := ioutil.WriteFile(binlogInfoFile, []byte("mysql-bin.000002\t1855\t33209bc5-c64a-11e7-b8ec-ee6f44cbb206:1-5\n"), 0600)
			Expect(err).NotTo(HaveOccurred())

			mock.ExpectExec(`RESET MASTER`).WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectExec(`SET GLOBAL gtid_purged = \?`).
				WithArgs("33209bc5-c64a-11e7-b8ec-ee6f44cbb206:1-5").
				WillReturnError(errors.New("some-db-error"))

			err = db.ApplyGTIDPurged()
			Expect(err).To(MatchError("some-db-error"))
		})

		It("returns a error when reading binlog info file fails", func() {
			err := db.ApplyGTIDPurged()
			Expect(err).To(HaveOccurred())
			Expect(os.IsNotExist(err)).To(BeTrue())
		})
	})

	Describe("WaitForReceivedTransactions", func() {
		BeforeEach(func() {
			cfg.ReplicationWaitSeconds = 2
		})

		It("returns no error when transactions are successfully received", func() {
			mock.ExpectQuery(`SELECT GTID_SUBSET`).
				WithArgs("some-other-gtid:1-10").
				WillReturnRows(sqlmock.NewRows([]string{"GTID_SUBSET('some-gtid:1-5', 'some-other-gtid:1-10')"}).
					AddRow("1"))

			err := db.WaitForReceivedTransactions("some-other-gtid:1-10")
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns an error when running the query fails", func() {
			mock.ExpectQuery(`SELECT GTID_SUBSET`).
				WithArgs("some-other-gtid:1-10").
				WillReturnError(errors.New("some-error"))

			err := db.WaitForReceivedTransactions("some-other-gtid:1-10")
			Expect(err).To(MatchError("timed out waiting for received transactions: some-error"))
		})

		It("returns an error when transactions are never received", func() {
			mock.ExpectQuery(`SELECT GTID_SUBSET`).
				WithArgs("some-other-gtid:1-10").
				WillReturnRows(sqlmock.NewRows([]string{"GTID_SUBSET('some-gtid:1-5', 'some-other-gtid:1-10')"}).
					AddRow("0"))

			err := db.WaitForReceivedTransactions("some-other-gtid:1-10")
			Expect(err).To(MatchError("timed out waiting for received transactions: some-other-gtid:1-10"))
		})

		It("returns an error when replication is not configured", func() {
			mock.ExpectQuery(`SELECT GTID_SUBSET`).
				WithArgs("some-other-gtid:1-10").
				WillReturnRows(sqlmock.NewRows([]string{"is_subset"}))

			err := db.WaitForReceivedTransactions("some-other-gtid:1-10")
			Expect(err).To(MatchError(`replication is not configured on this instance`))
		})

		Context("when timeouts are misconfigured", func() {
			BeforeEach(func() {
				cfg.ReplicationWaitSeconds = 0
			})
			It("returns an error when a timeout before any queries are run", func() {
				err := db.WaitForReceivedTransactions("some-other-gtid:1-10")
				Expect(err).To(MatchError(`timed out waiting for received transactions: some-other-gtid:1-10`))
			})
		})
	})

	Describe("MakeReadOnly", func() {
		It("makes the db read only", func() {
			mock.ExpectExec(`SET GLOBAL super_read_only`).
				WillReturnResult(sqlmock.NewResult(1, 1))

			Expect(
				db.MakeReadOnly(),
			).To(Succeed())
		})

		It("returns an error when the db fails", func() {
			mock.ExpectExec(`SET GLOBAL super_read_only`).
				WillReturnError(errors.New("some-error"))

			Expect(db.MakeReadOnly()).To(MatchError("some-error"))
		})
	})

	Describe("MakeFollower", func() {
		BeforeEach(func() {
			cfg.PeerAddress = "PEER-ADDRESS"
			cfg.ReplicationUser = "ReplicationUser"
			cfg.ReplicationPassword = "ReplicationPassword"
		})

		It("makes the db a follower of its peer", func() {
			mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`RESET SLAVE ALL`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`CHANGE MASTER TO MASTER_HOST`).
				WithArgs("PEER-ADDRESS", "ReplicationUser", "ReplicationPassword").
				WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(`START SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))

			Expect(db.MakeFollower()).To(Succeed())
		})

		It("returns errors made by the db", func() {
			mock.ExpectExec(`STOP SLAVE`).WillReturnError(errors.New("some-error"))

			Expect(db.MakeFollower()).To(MatchError("some-error"))
		})

		It("returns errors made by the db", func() {
			mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`RESET SLAVE ALL`).WillReturnError(errors.New("some-error"))

			Expect(db.MakeFollower()).To(MatchError("some-error"))
		})

		It("returns errors made by the db", func() {
			mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`RESET SLAVE ALL`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`CHANGE MASTER TO MASTER_HOST`).
				WithArgs("PEER-ADDRESS", "ReplicationUser", "ReplicationPassword").
				WillReturnError(errors.New("some-error"))

			Expect(db.MakeFollower()).To(MatchError("some-error"))
		})

		It("returns errors made by the db", func() {
			mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`RESET SLAVE ALL`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`CHANGE MASTER TO MASTER_HOST`).
				WithArgs("PEER-ADDRESS", "ReplicationUser", "ReplicationPassword").
				WillReturnError(errors.New("some-error"))

			Expect(db.MakeFollower()).To(MatchError("some-error"))
		})

		It("returns errors made by the db", func() {
			mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`RESET SLAVE ALL`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`CHANGE MASTER TO MASTER_HOST`).
				WithArgs("PEER-ADDRESS", "ReplicationUser", "ReplicationPassword").
				WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(`START SLAVE`).WillReturnError(errors.New("some-error"))

			Expect(db.MakeFollower()).To(MatchError("some-error"))
		})

		Context("when a mysql ca is available with content", func() {

			var caFilePath string

			BeforeEach(func() {
				var err error
				f, err := ioutil.TempFile("", "mysql_")
				Expect(err).NotTo(HaveOccurred())

				caFilePath = f.Name()

				ioutil.WriteFile(caFilePath, []byte("some-mysql-ca-content"), 0600)
				cfg.MySQLCACertPath = caFilePath
			})

			AfterEach(func() {
				os.RemoveAll(caFilePath)
			})

			It("configures replication with the ca", func() {
				mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`RESET SLAVE ALL`).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`CHANGE MASTER TO MASTER_HOST.*MASTER_SSL_CA = ?.*MASTER_SSL_VERIFY_SERVER_CERT = 0`).
					WithArgs("PEER-ADDRESS", "ReplicationUser", "ReplicationPassword", caFilePath).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(`START SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))

				Expect(db.MakeFollower()).To(Succeed())
			})
		})
	})

	Describe("WaitForReplication", func() {

		BeforeEach(func() {
			cfg.ReplicationWaitSeconds = 3
		})

		It("succeeds if replication is healthy", func() {
			mock.ExpectQuery(`SELECT.*`).
				WillReturnRows(sqlmock.NewRows([]string{"connection_state", "applier_state"}).
					FromCSVString("ON,ON"))

			err := db.WaitForReplication()
			Expect(err).NotTo(HaveOccurred())
		})

		It("Returns error state and replication error at once if replication state is bad", func() {
			mock.ExpectQuery(`SELECT.*connection_state.*`).
				WillReturnRows(sqlmock.NewRows([]string{"connection_state", "applier_state"}).
					FromCSVString("CONNECTING,OFF"))
			mock.ExpectQuery(`SELECT.*replication_connection_status.*`).
				WillReturnRows(sqlmock.NewRows([]string{"last_error_number", "last_error_message"}).
					FromCSVString("1234,connection status error"))
			mock.ExpectQuery(`SELECT.*replication_applier_status_by_worker.*`).
				WillReturnRows(sqlmock.NewRows([]string{"last_error_number", "last_error_message"}).
					FromCSVString("4321,applier status error"))

			err := db.WaitForReplication()
			Expect(err).To(MatchError(ContainSubstring(`Bad replication state: &{ConnectionState:CONNECTING ApplierState:OFF}`)))
			Expect(err).To(MatchError(ContainSubstring(`Replication connection error: 1234 connection status error`)))
			Expect(err).To(MatchError(ContainSubstring(`Replication applier error: 4321 applier status error`)))
		})

		It("Returns error state at once if the query returns an error", func() {
			mock.ExpectQuery(`SELECT.*`).WillReturnError(errors.New("SQL Error"))

			err := db.WaitForReplication()
			Expect(err).To(MatchError("SQL Error"))
		})

		It("Returns healthy state after one attempt", func() {
			mock.ExpectQuery(`SELECT.*`).
				WillReturnRows(sqlmock.NewRows([]string{"connection_state", "applier_state"}).
					FromCSVString("CONNECTING,ON"))

			mock.ExpectQuery(`SELECT.*`).
				WillReturnRows(sqlmock.NewRows([]string{"connection_state", "applier_state"}).
					FromCSVString("ON,ON"))

			err := db.WaitForReplication()
			Expect(err).NotTo(HaveOccurred())
		})

		It("Times out waiting for state", func() {
			mock.ExpectQuery(`SELECT.*`).
				WillReturnRows(sqlmock.NewRows([]string{"connection_state", "applier_state"}).
					FromCSVString("CONNECTING,ON"))

			mock.ExpectQuery(`SELECT.*`).
				WillReturnRows(sqlmock.NewRows([]string{"connection_state", "applier_state"}).
					FromCSVString("CONNECTING,ON"))

			err := db.WaitForReplication()
			Expect(err).To(MatchError("Timeout waiting for healthy replication state"))
		})
	})
})
