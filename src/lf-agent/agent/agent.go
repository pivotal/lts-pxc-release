package agent

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"

	"lf-agent/config"
)

//go:generate counterfeiter . DatabaseClient
type DatabaseClient interface {
	WaitForReplication() error
	MakeFollower() error
	MakeReadOnly() error
	Status() (*DBStatus, error)
	WaitForReceivedTransactions(gtidExecuted string) error
}

//go:generate counterfeiter . Jumpstart
type Jumpstart interface {
	Prepare() error
	Perform() error
}

type LFAgent struct {
	DB                     *sql.DB
	ReplicationTimeout     time.Duration
	ReplicationPollingTime time.Duration
	Config                 config.Config
	jmpstart               Jumpstart
	databaseClient         DatabaseClient
	logger                 *log.Logger
}

type ReplicationMode int

const (
	ReplicationAsync ReplicationMode = iota
	ReplicationSemisync
)

func (r ReplicationMode) String() string {
	switch r {
	case ReplicationSemisync:
		return "semisync"
	default:
		return "async"
	}
}

const lfConfig = `
[mysqld]
super-read-only = OFF
`

func NewLFAgent(db *sql.DB, databaseClient DatabaseClient, jmpstart Jumpstart, cfg config.Config) *LFAgent {
	return &LFAgent{
		ReplicationTimeout:     30 * time.Second,
		ReplicationPollingTime: time.Second,
		logger:                 log.New(os.Stdout, "[agent] ", log.LstdFlags),

		DB:             db,
		Config:         cfg,
		jmpstart:       jmpstart,
		databaseClient: databaseClient,
	}
}

func (lfa *LFAgent) MakeLeader(failover bool) error {
	lfConf := lfConfig

	if err := lfa.waitForReplicationReplay(); err != nil {
		return err
	}

	if err := disableReplication(lfa.DB); err != nil {
		return err
	}

	if err := setWritable(lfa.DB); err != nil {
		return err
	}

	if lfa.Config.ReplicationMode == "semi-sync" && !failover {
		lfConf += "\nloose-rpl-semi-sync-master-enabled = ON\n"
		if err := enableSemiSync(lfa.DB); err != nil {
			return err
		}
	}

	if err := ioutil.WriteFile(path.Join(lfa.Config.LfStateDir, "leader.cnf"), []byte(lfConf), 0600); err != nil {
		return err
	}

	return nil
}

func (lfa *LFAgent) CheckIfPromotable() error {
	if err := checkReplicationApplierState(lfa.DB); err != nil {
		return err
	}

	if err := checkReplicationConnectionState(lfa.DB); err != nil {
		return err
	}

	return nil
}

func (lfa *LFAgent) MySQLStatus() (*DBStatus, error) {
	return lfa.databaseClient.Status()
}

func (lfa *LFAgent) waitForReplicationReplay() error {
	var (
		query = `
		SELECT WAIT_FOR_EXECUTED_GTID_SET(
			RECEIVED_TRANSACTION_SET, ?
		) AS timeout
		FROM performance_schema.replication_connection_status
		WHERE CHANNEL_NAME = ''`

		waitTimedOut bool
	)

	err := lfa.DB.QueryRow(query, lfa.Config.ReplicationWaitSeconds).Scan(&waitTimedOut)
	if err == sql.ErrNoRows {
		return nil
	}

	if err != nil {
		return err
	}

	if waitTimedOut {
		return fmt.Errorf("Timed out after %d seconds. The follower is still applying MySQL transactions. Please re-run the make-leader errand.", lfa.Config.ReplicationWaitSeconds)
	}

	return nil
}

func disableReplication(db *sql.DB) error {
	if _, err := db.Exec(`STOP SLAVE`); err != nil {
		return err
	}

	if _, err := db.Exec(`RESET SLAVE ALL`); err != nil {
		return err
	}

	return nil
}

func setWritable(db *sql.DB) error {
	_, err := db.Exec(`SET GLOBAL read_only = off`)

	return err
}

func enableSemiSync(db *sql.DB) error {
	_, err := db.Exec(`SET GLOBAL rpl_semi_sync_master_enabled = on`)

	return err
}

func checkReplicationApplierState(db *sql.DB) error {
	var (
		query = `
		SELECT SERVICE_STATE
		FROM performance_schema.replication_applier_status
		WHERE CHANNEL_NAME = ''
		`
		state string
	)
	err := db.QueryRow(query).Scan(&state)
	if err == sql.ErrNoRows {
		return nil
	}

	if err != nil {
		return err
	}

	if state == "OFF" {
		return errors.New("Replication settings exist on this instance and Slave SQL Thread is turned off. Fix replication settings and try again.")
	}

	return nil
}

func checkReplicationConnectionState(db *sql.DB) error {
	var (
		serviceState string
		repConnQuery = `SELECT SERVICE_STATE
		FROM performance_schema.replication_connection_status
		WHERE CHANNEL_NAME = ''`
	)

	err := db.QueryRow(repConnQuery).Scan(&serviceState)
	if err == sql.ErrNoRows {
		return nil
	}

	if err != nil {
		return err
	}

	if serviceState == "ON" {
		return errors.New(
			"Leader VM is still accessible from the follower. Refusing to promote to avoid data divergence",
		)
	}

	return nil
}
