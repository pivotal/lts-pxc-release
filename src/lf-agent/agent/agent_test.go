package agent_test

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	"lf-agent/agent"
	"lf-agent/config"

	"lf-agent/agent/agentfakes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Agent", func() {
	Context("MakeLeader", func() {
		var (
			db                      *sql.DB
			lfa                     *agent.LFAgent
			lfStateDir              string
			mock                    sqlmock.Sqlmock
			connectionStatusErrCols = []string{"LAST_ERROR_MESSAGE"}
		)
		BeforeEach(func() {
			var err error
			lfStateDir, err = ioutil.TempDir("", "api_server")
			Expect(err).ToNot(HaveOccurred())

			db, mock, err = sqlmock.New()
			Expect(err).NotTo(HaveOccurred())

			lfa = agent.NewLFAgent(db, nil, nil, config.Config{
				LfStateDir: lfStateDir,
			})
		})

		AfterEach(func() {
			Expect(mock.ExpectationsWereMet()).To(Succeed())

			err := os.RemoveAll(lfStateDir)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("CheckIfPromotable", func() {
			Context("when the replication applier thread is not running", func() {
				It("returns an error message", func() {
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_applier_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols).AddRow("OFF"))

					err := lfa.CheckIfPromotable()
					Expect(err).To(MatchError("Replication settings exist on this instance and Slave SQL Thread is turned off. Fix replication settings and try again."))
				})
			})

			Context("when the replication connection thread is not running", func() {
				It("returns false with no error message", func() {
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_applier_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols).AddRow("ON"))
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols).AddRow("OFF"))

					err := lfa.CheckIfPromotable()
					Expect(err).To(BeNil())
				})
			})

			Context("when the replication connection status is attempting to connect", func() {
				It("returns false with no error message", func() {
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_applier_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols).AddRow("ON"))
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols).AddRow("CONNECTING"))

					err := lfa.CheckIfPromotable()
					Expect(err).To(BeNil())
				})
			})

			Context("when the replication connection status is running", func() {
				It("returns true with an error message", func() {
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_applier_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols).AddRow("ON"))
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols).AddRow("ON"))

					err := lfa.CheckIfPromotable()
					Expect(err).To(MatchError("Leader VM is still accessible from the follower. Refusing to promote to avoid data divergence"))
				})
			})

			Context("when there are no rows returned", func() {
				It("returns false and no error message", func() {
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_applier_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols))
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols))

					err := lfa.CheckIfPromotable()
					Expect(err).To(BeNil())
				})
			})

			Context("when an unexpected error occurs", func() {
				It("returns false with an error message", func() {
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_applier_status`).
						WillReturnRows(sqlmock.NewRows(connectionStatusErrCols))
					mock.ExpectQuery(`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status`).
						WillReturnError(errors.New("some-error"))

					err := lfa.CheckIfPromotable()
					Expect(err).To(MatchError("some-error"))
				})
			})
		})

		It("Promotes an instance with MakeLeader", func() {
			mock.ExpectQuery(`SELECT WAIT_FOR_EXECUTED_GTID_SET`).
				WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow("0"))
			mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`RESET SLAVE ALL`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`SET GLOBAL read_only = off`).WillReturnResult(sqlmock.NewResult(1, 1))
			err := lfa.MakeLeader(false)
			Expect(err).NotTo(HaveOccurred())

			leaderCnf, err := ioutil.ReadFile(filepath.Join(lfStateDir, "leader.cnf"))
			Expect(err).NotTo(HaveOccurred())

			Expect(leaderCnf).To(ContainSubstring("[mysqld]"))
			Expect(leaderCnf).To(ContainSubstring("super-read-only = OFF"))
		})

		It("Returns and error if replication replay times out", func() {
			lfa.Config.ReplicationWaitSeconds = 10

			mock.ExpectQuery(`SELECT WAIT_FOR_EXECUTED_GTID_SET`).
				WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow("1"))
			err := lfa.MakeLeader(false)
			Expect(err).To(MatchError("Timed out after 10 seconds. The follower is still applying MySQL transactions. Please re-run the make-leader errand."))
		})

		It("Returns an error if the directory does not exist", func() {
			mock.ExpectQuery(`SELECT WAIT_FOR_EXECUTED_GTID_SET`).
				WillReturnError(sql.ErrNoRows)
			mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`RESET SLAVE ALL`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`SET GLOBAL read_only = off`).WillReturnResult(sqlmock.NewResult(1, 1))

			lfa.Config.LfStateDir = "/non/existent/directory"
			err := lfa.MakeLeader(false)
			Expect(err).To(MatchError("open /non/existent/directory/leader.cnf: no such file or directory"))
		})

		Context("When replication_mode is set to semi-sync", func() {
			It("Promotes an instance with MakeLeader in normal mode, setting rpl_semi_sync_master_enabled", func() {
				mock.ExpectQuery(`SELECT WAIT_FOR_EXECUTED_GTID_SET`).
					WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow("0"))
				mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`RESET SLAVE ALL`).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`SET GLOBAL read_only = off`).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`SET GLOBAL rpl_semi_sync_master_enabled = on`).WillReturnResult(sqlmock.NewResult(1, 1))

				lfa.Config.ReplicationMode = "semi-sync"
				err := lfa.MakeLeader(false)
				Expect(err).NotTo(HaveOccurred())

				leaderCnf, err := ioutil.ReadFile(filepath.Join(lfStateDir, "leader.cnf"))
				Expect(err).NotTo(HaveOccurred())

				Expect(leaderCnf).To(ContainSubstring("[mysqld]"))
				Expect(leaderCnf).To(ContainSubstring("super-read-only = OFF"))
				Expect(leaderCnf).To(ContainSubstring("rpl-semi-sync-master-enabled = ON"))
			})
			It("Promotes an instance with MakeLeader in failover mode, not setting rpl_semi_sync_master_enabled", func() {
				mock.ExpectQuery(`SELECT WAIT_FOR_EXECUTED_GTID_SET`).
					WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow("0"))
				mock.ExpectExec(`STOP SLAVE`).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`RESET SLAVE ALL`).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`SET GLOBAL read_only = off`).WillReturnResult(sqlmock.NewResult(1, 1))

				lfa.Config.ReplicationMode = "semi-sync"
				err := lfa.MakeLeader(true)
				Expect(err).NotTo(HaveOccurred())

				leaderCnf, err := ioutil.ReadFile(filepath.Join(lfStateDir, "leader.cnf"))
				Expect(err).NotTo(HaveOccurred())

				Expect(leaderCnf).To(ContainSubstring("[mysqld]"))
				Expect(leaderCnf).To(ContainSubstring("super-read-only = OFF"))
				Expect(leaderCnf).NotTo(ContainSubstring("rpl-semi-sync-master-enabled = ON"))
			})
		})
	})

	Context("MySQLStatus", func() {
		var (
			lfa    *agent.LFAgent
			dbmock *agentfakes.FakeDatabaseClient
		)

		BeforeEach(func() {
			dbmock = new(agentfakes.FakeDatabaseClient)
			lfa = agent.NewLFAgent(nil, dbmock, nil, config.Config{})
		})

		It("returns status from the DB", func() {
			status := &agent.DBStatus{
				IPAddress:             "10.0.0.1",
				ReadOnly:              true,
				ReplicationConfigured: false,
				ReplicationMode:       agent.ReplicationAsync,
				GtidExecuted:          "some-gtid-executed",
			}

			dbmock.StatusReturns(status, nil)
			Expect(lfa.MySQLStatus()).To(Equal(status))
		})

		It("returns an error getting the db-status fails", func() {
			dbmock.StatusReturns(nil, errors.New("some-error"))

			_, err := lfa.MySQLStatus()
			Expect(err).To(MatchError("some-error"))
		})
	})
})
