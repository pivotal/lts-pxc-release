package agent_test

import (
	"database/sql"
	"errors"

	"lf-agent/agent"
	"lf-agent/config"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("Agent", func() {
	Context("Heartbeats", func() {
		var (
			db   *sql.DB
			lfa  *agent.LFAgent
			mock sqlmock.Sqlmock
		)
		BeforeEach(func() {
			var err error

			db, mock, err = sqlmock.New()
			Expect(err).NotTo(HaveOccurred())

			lfa = agent.NewLFAgent(db, nil, nil, config.Config{
				EnableHeartbeats: true,
			})
		})

		AfterEach(func() {
			Expect(mock.ExpectationsWereMet()).To(Succeed())
		})

		Context("When heartbeats are enabled", func() {
			It("setups heartbeats", func() {
				mock.ExpectExec(`CREATE DATABASE IF NOT EXISTS replication_monitoring`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`CREATE TABLE IF NOT EXISTS replication_monitoring.heartbeat .*`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`REPLACE INTO replication_monitoring.heartbeat .*`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`CREATE EVENT IF NOT EXISTS replication_monitoring.update_heartbeat .*`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`ALTER EVENT replication_monitoring.update_heartbeat ENABLE`).
					WillReturnResult(sqlmock.NewResult(1, 1))

				Expect(lfa.ToggleHeartbeats()).To(Succeed())
			})
			It("returns an error when heartbeat db creation fails", func() {
				mock.ExpectExec(`CREATE DATABASE IF NOT EXISTS replication_monitoring`).
					WillReturnError(errors.New("Creating heartbeat DB failed"))
				Expect(lfa.ToggleHeartbeats()).To(MatchError("Creating heartbeat DB failed"))
			})
			It("returns an error when heartbeat table creation fails", func() {
				mock.ExpectExec(`CREATE DATABASE IF NOT EXISTS replication_monitoring`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`CREATE TABLE IF NOT EXISTS replication_monitoring.heartbeat .*`).
					WillReturnError(errors.New("Creating heartbeat table failed"))
				Expect(lfa.ToggleHeartbeats()).To(MatchError("Creating heartbeat table failed"))
			})

			It("returns an error when heartbeat table seeding fails", func() {
				mock.ExpectExec(`CREATE DATABASE IF NOT EXISTS replication_monitoring`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`CREATE TABLE IF NOT EXISTS replication_monitoring.heartbeat .*`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`REPLACE INTO replication_monitoring.heartbeat .*`).
					WillReturnError(errors.New("Seeding heartbeat table failed"))

				Expect(lfa.ToggleHeartbeats()).To(MatchError("Seeding heartbeat table failed"))
			})
			It("returns an error when heartbeat event creation fails", func() {
				mock.ExpectExec(`CREATE DATABASE IF NOT EXISTS replication_monitoring`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`CREATE TABLE IF NOT EXISTS replication_monitoring.heartbeat .*`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`REPLACE INTO replication_monitoring.heartbeat .*`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`CREATE EVENT IF NOT EXISTS replication_monitoring.update_heartbeat .*`).
					WillReturnError(errors.New("Heartbeat event creation failed"))

				Expect(lfa.ToggleHeartbeats()).To(MatchError("Heartbeat event creation failed"))
			})
			It("returns an error when heartbeat event enabling fails", func() {
				mock.ExpectExec(`CREATE DATABASE IF NOT EXISTS replication_monitoring`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`CREATE TABLE IF NOT EXISTS replication_monitoring.heartbeat .*`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`REPLACE INTO replication_monitoring.heartbeat .*`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`CREATE EVENT IF NOT EXISTS replication_monitoring.update_heartbeat .*`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`ALTER EVENT replication_monitoring.update_heartbeat ENABLE`).
					WillReturnError(errors.New("Heartbeat event enabling failed"))
				Expect(lfa.ToggleHeartbeats()).To(MatchError("Heartbeat event enabling failed"))
			})
		})
		Context("When heartbeats are disabled", func() {
			BeforeEach(func() {
				lfa.Config.EnableHeartbeats = false
			})
			It("Succeeds", func() {
				Expect(lfa.ToggleHeartbeats()).To(Succeed())
			})
		})
	})
})
