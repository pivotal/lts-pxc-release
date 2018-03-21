package database

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"lf-agent/agent"
	"lf-agent/config"

	"github.com/pkg/errors"
)

type Database struct {
	driver *sql.DB
	Cfg    config.Config
}

func NewDatabase(driver *sql.DB, cfg config.Config) *Database {
	return &Database{
		driver: driver,
		Cfg:    cfg,
	}
}

func (d *Database) WaitForReceivedTransactions(gtidExecuted string) error {
	var (
		timeout   = time.Duration(d.Cfg.ReplicationWaitSeconds) * time.Second
		timeoutCh = time.After(timeout)
		ticker    = time.NewTicker(time.Second)
		err       = errors.New(gtidExecuted)
	)

	defer ticker.Stop()

	for {
		select {
		case <-timeoutCh:
			return errors.Wrap(err, "timed out waiting for received transactions")
		case <-ticker.C:
			var isTransactionsReceived bool
			// Check that the executed + received transactions have caught up to the peer's transactions
			query := `SELECT GTID_SUBSET(?, CONCAT(@@global.gtid_executed, ',', RECEIVED_TRANSACTION_SET))
			FROM performance_schema.replication_connection_status
			WHERE CHANNEL_NAME = ''
			`
			err = d.driver.QueryRow(query, gtidExecuted).Scan(&isTransactionsReceived)

			if err == sql.ErrNoRows {
				return errors.New("replication is not configured on this instance")
			}

			if err != nil {
				continue
			}

			if isTransactionsReceived {
				return nil
			} else {
				err = errors.New(gtidExecuted)
			}
		}
	}
}

func (d *Database) Status() (*agent.DBStatus, error) {
	var status = &agent.DBStatus{IPAddress: d.Cfg.HostAddress, ReplicationMode: agent.ReplicationAsync}

	if err := d.driver.QueryRow(`SELECT @@global.gtid_executed`).Scan(&status.GtidExecuted); err != nil {
		return nil, errors.Wrap(err, "could not retrieve db-status")
	}

	if err := d.driver.QueryRow(`SELECT @@global.super_read_only`).Scan(&status.ReadOnly); err != nil {
		return nil, errors.Wrap(err, "could not retrieve db-status")
	}

	if err := d.driver.QueryRow(`SELECT COUNT(*) = 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME = ''`).Scan(&status.ReplicationConfigured); err != nil {
		return nil, errors.Wrap(err, "could not retrieve db-status")
	}

	var semisyncPluginsLoaded int
	if err := d.driver.QueryRow(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%semi%'`).Scan(&semisyncPluginsLoaded); err != nil {
		return nil, errors.Wrap(err, "could not retrieve db-status")
	}

	if semisyncPluginsLoaded > 0 {
		var semisyncMasterStatus string
		if err := d.driver.QueryRow(`SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Rpl_semi_sync_master_status'`).Scan(&semisyncMasterStatus); err != nil {
			return nil, errors.Wrap(err, "could not retrieve db-status")
		}

		var semisyncSlaveStatus string
		if err := d.driver.QueryRow(`SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Rpl_semi_sync_slave_status'`).Scan(&semisyncSlaveStatus); err != nil {
			return nil, errors.Wrap(err, "could not retrieve db-status")
		}

		if semisyncMasterStatus == "ON" || semisyncSlaveStatus == "ON" {
			status.ReplicationMode = agent.ReplicationSemisync
		}
	}

	return status, nil
}

func (d *Database) ApplyGTIDPurged() error {
	contents, err := ioutil.ReadFile(filepath.Join(d.Cfg.Datadir, "xtrabackup_binlog_info"))
	if err != nil {
		return err
	}

	fields := strings.SplitN(string(contents), "\t", 3)

	if len(fields) != 3 {
		return errors.New("gtid_purged not found in xtrabackup_binlog_info")
	}

	gtidPurged := strings.TrimSpace(fields[2])

	if _, err = d.driver.Exec("RESET MASTER"); err != nil {
		return err
	}

	_, err = d.driver.Exec("SET GLOBAL gtid_purged = ?", gtidPurged)
	return err
}

func (d *Database) MakeReadOnly() error {
	_, err := d.driver.Exec("SET GLOBAL super_read_only = on")
	return err
}

func (d *Database) MakeFollower() error {
	if _, err := d.driver.Exec(`STOP SLAVE`); err != nil {
		return err
	}

	if _, err := d.driver.Exec(`RESET SLAVE ALL`); err != nil {
		return err
	}

	query, args := d.getConfigureReplicationQueryAndArgs()

	if _, err := d.driver.Exec(query, args...); err != nil {
		return err
	}

	_, err := d.driver.Exec(`START SLAVE`)
	return err
}

func (d *Database) getConfigureReplicationQueryAndArgs() (string, []interface{}) {
	var (
		query = `CHANGE MASTER TO MASTER_HOST = ?, MASTER_USER = ?, MASTER_PASSWORD = ?, MASTER_AUTO_POSITION = 1, MASTER_SSL = 1, MASTER_TLS_VERSION = 'TLSv1.2'`
		args  = []interface{}{d.Cfg.PeerAddress, d.Cfg.ReplicationUser, d.Cfg.ReplicationPassword}
	)

	if f, err := os.Stat(d.Cfg.MySQLCACertPath); (err == nil) && (f.Size() > 0) {
		query += `, MASTER_SSL_CA = ?, MASTER_SSL_VERIFY_SERVER_CERT = 0`
		args = append(args, d.Cfg.MySQLCACertPath)
	}

	return query, args
}

type ReplicationState struct {
	ConnectionState string
	ApplierState    string
}

func (d *Database) WaitForReplication() error {
	var (
		timeout   = time.Duration(d.Cfg.ReplicationWaitSeconds) * time.Second
		timeoutCh = time.After(timeout)
		ticker    = time.NewTicker(time.Second)
	)

	defer ticker.Stop()

	for {
		select {
		case <-timeoutCh:
			return errors.New("Timeout waiting for healthy replication state")
		case <-ticker.C:
			state, err := d.getReplicationState()
			if err != nil {
				return err
			}

			switch *state {
			case ReplicationState{ConnectionState: "ON", ApplierState: "ON"}:
				return nil
			case ReplicationState{ConnectionState: "CONNECTING", ApplierState: "ON"}:
				continue
			default:
				return d.getReplicationErrors(state)
			}
		}
	}
}

func (d *Database) getReplicationState() (*ReplicationState, error) {
	query := `SELECT
	(SELECT SERVICE_STATE
	FROM performance_schema.replication_connection_status
	WHERE CHANNEL_NAME = '') connection_state,
	(SELECT SERVICE_STATE
	FROM performance_schema.replication_applier_status_by_worker
	WHERE CHANNEL_NAME = '') applier_state`
	var connectionState, applierState sql.NullString
	err := d.driver.QueryRow(query).
		Scan(&connectionState, &applierState)
	if err != nil {
		return nil, err
	}

	if !connectionState.Valid || !applierState.Valid {
		return nil, errors.New("Replication appears to be unconfigured")
	}
	return &ReplicationState{ConnectionState: connectionState.String, ApplierState: applierState.String}, err
}

func (d *Database) getReplicationErrors(state *ReplicationState) error {
	var connectionErrorStatusNumber, connectionError, applierErrorStatusNumber, applierError string
	err := d.driver.QueryRow(`SELECT LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE FROM performance_schema.replication_connection_status`).
		Scan(&connectionErrorStatusNumber, &connectionError)
	if err != nil {
		return err
	}
	err = d.driver.QueryRow(`SELECT LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE FROM performance_schema.replication_applier_status_by_worker`).
		Scan(&applierErrorStatusNumber, &applierError)
	if err != nil {
		return err
	}
	errorMsg := `
	Bad replication state: %+v
	Replication connection error: %s %s
	Replication applier error: %s %s
	`
	return errors.Errorf(errorMsg, state, connectionErrorStatusNumber, connectionError, applierErrorStatusNumber, applierError)
}
