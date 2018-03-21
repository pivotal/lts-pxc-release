package agent

import "database/sql"

func (lfa *LFAgent) ToggleHeartbeats() error {
	if lfa.Config.EnableHeartbeats {
		return enableHeartbeats(lfa.DB)
	}
	return nil
}

func enableHeartbeats(db *sql.DB) error {
	if err := createHeartbeatDatabase(db); err != nil {
		return err
	}

	if err := createHeartbeatTable(db); err != nil {
		return err
	}

	if err := seedHeartbeatTable(db); err != nil {
		return err
	}

	if err := createHeartbeatEvent(db); err != nil {
		return err
	}

	return enableHeartbeatEvent(db)
}

func createHeartbeatDatabase(db *sql.DB) error {
	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS replication_monitoring")
	return err
}

func createHeartbeatTable(db *sql.DB) error {
	ddl := `
	CREATE TABLE IF NOT EXISTS replication_monitoring.heartbeat (
		server_id int unsigned PRIMARY KEY,
		ts TIMESTAMP(6) NOT NULL,
		timestamp TIMESTAMP AS (ts) VIRTUAL NOT NULL)`
	_, err := db.Exec(ddl)
	return err
}

func seedHeartbeatTable(db *sql.DB) error {
	dml := "REPLACE INTO replication_monitoring.heartbeat (server_id, ts) VALUES (@@global.server_id, NOW())"
	_, err := db.Exec(dml)
	return err
}

func createHeartbeatEvent(db *sql.DB) error {
	ddl := `
	CREATE EVENT IF NOT EXISTS replication_monitoring.update_heartbeat
    ON SCHEDULE EVERY 5 SECOND
    DISABLE
    DO
        BEGIN
			IF @@global.read_only = 0 THEN
				UPDATE heartbeat
				SET ts = NOW()
				WHERE server_id = @@global.server_id;
			END IF;
        END`
	_, err := db.Exec(ddl)
	return err
}

func enableHeartbeatEvent(db *sql.DB) error {
	_, err := db.Exec("ALTER EVENT replication_monitoring.update_heartbeat ENABLE")
	return err
}
