package test_helpers

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/onsi/gomega"
)

func DbSetup(db *sql.DB, tableName string) string {
	var (
		dbName = "pxc_release_test_db"
		err    error
	)

	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS pxc_release_test_db`)
	Expect(err).NotTo(HaveOccurred())

	statement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS pxc_release_test_db.%s (test_data varchar(255) PRIMARY KEY)", tableName)
	_, err = db.Exec(statement)
	Expect(err).NotTo(HaveOccurred())
	return dbName
}

func DbConnNoDb() *sql.DB {
	var mysqlUsername = os.Getenv("MYSQL_USERNAME")
	var mysqlPassword = os.Getenv("MYSQL_PASSWORD")

	pxcConnectionString := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/",
		mysqlUsername,
		mysqlPassword,
		DbHost(),
		3306)

	databaseConnection, err := sql.Open("mysql", pxcConnectionString)
	Expect(err).NotTo(HaveOccurred())

	return databaseConnection
}

func DbConn() *sql.DB {
	var mysqlUsername = os.Getenv("MYSQL_USERNAME")
	var mysqlPassword = os.Getenv("MYSQL_PASSWORD")

	return DbConnWithUser(mysqlUsername, mysqlPassword, DbHost())
}

func DbConnWithUser(mysqlUsername, mysqlPassword, mysqlHost string) *sql.DB {
	pxcConnectionString := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/",
		mysqlUsername,
		mysqlPassword,
		mysqlHost,
		3306)

	databaseConnection, err := sql.Open("mysql", pxcConnectionString)
	Expect(err).NotTo(HaveOccurred())

	return databaseConnection
}

func DbCleanup(db *sql.DB) {
	statement := "DROP DATABASE pxc_release_test_db"
	_, err := db.Exec(statement)
	Expect(err).NotTo(HaveOccurred())
}

func DbHost() string {
	dbHost, hostExists := os.LookupEnv("MYSQL_HOST")
	if hostExists {
		return dbHost
	}
	return os.Getenv("BOSH_ENVIRONMENT")
}
