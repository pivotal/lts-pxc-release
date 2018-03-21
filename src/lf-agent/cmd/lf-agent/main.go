package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"lf-agent/agent"
	"lf-agent/config"
	"lf-agent/runner"
	"lf-agent/server"

	"lf-agent/jumpstart"
	"lf-agent/monit"

	"lf-agent/database"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	configPath := os.Args[1]

	cfg, err := config.NewConfig(configPath)
	if err != nil {
		log.Fatalf("Error in config: %s\n", err)
	}

	dsn := fmt.Sprintf("%s:%s@unix(/tmp/mysql.sock)/?interpolateParams=true", cfg.ReplicationAdminUser, cfg.ReplicationAdminPassword)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error connecting to db: %s", err)
	}
	defer db.Close()

	dbClient := database.NewDatabase(
		db,
		cfg,
	)

	monitClient := monit.NewClient(
		"http://127.0.0.1:2822",
		"vcap",
		"random-password",
		time.Minute,
	)

	streamingBackupClient := jumpstart.NewStreamingBackupClient(
		cfg,
	)

	jmpstart := jumpstart.NewJumpstart(
		dbClient,
		monitClient,
		runner.Runner{},
		streamingBackupClient,
		cfg,
	)

	agent := agent.NewLFAgent(db, dbClient, jmpstart, cfg)

	server := server.NewServer(agent, cfg)

	if err = server.Serve(); err != nil {
		log.Fatalf("Error in HTTP handler: %s\n", err)
	}
}
