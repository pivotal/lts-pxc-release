package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	rootPassword := os.Getenv("MYSQL_ROOT_PASSWORD")

	//Start mariadb
	mariadbCmd := exec.Command("/var/vcap/packages/mariadb/bin/mysqld_safe", "--defaults-file=/var/vcap/jobs/mysql/config/my.cnf", "--wsrep-on=OFF", "--wsrep-desync=ON", "--wsrep-OSU-method=RSU", "--wsrep-provider='none'", "--skip-networking")
	err := mariadbCmd.Start()
	if err != nil {
		panic(err)
	}

	//Wait for db
	time.Sleep(15 * time.Second)

	dsn := fmt.Sprintf("%s:%s@unix(%s)/", "root", rootPassword, "/var/vcap/sys/run/mysql/mysqld.sock")
	mariadbDatabaseConnection, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	dsn = fmt.Sprintf("%s:%s@unix(%s)/", "root", rootPassword, "/var/vcap/sys/run/mysql-clustered/mysqld.sock")
	pxcDatabaseConnection, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	// Get all the database names
	query := "select schema_name from information_schema.schemata where schema_name NOT IN ('performance_schema', 'mysql', 'information_schema')"

	rows, err := mariadbDatabaseConnection.Query(query)
	if err != nil {
		panic(err)
	}

	var databaseNames []string
	for rows.Next() {
		var databaseName string
		rows.Scan(&databaseName)
		databaseNames = append(databaseNames, databaseName)
	}

	// Get all the table names
	query = "select CONCAT(table_schema,'.',table_name) from information_schema.tables where table_schema NOT IN ('performance_schema', 'mysql', 'information_schema')"

	rows, err = mariadbDatabaseConnection.Query(query)
	if err != nil {
		panic(err)
	}

	var tableNames []string
	for rows.Next() {
		var tableName string
		rows.Scan(&tableName)
		tableNames = append(tableNames, tableName)
	}
	rows.Close()

	// Get all flush tables queries
	tableNamesCommas := strings.Join(tableNames, ",")
	flushTablesStatement := "FLUSH TABLES " + tableNamesCommas + " FOR EXPORT"

	_, err = mariadbDatabaseConnection.Exec(flushTablesStatement)
	if err != nil {
		panic(err)
	}

	// Get the .sql file that will create all the tables
	// /var/vcap/packages/mariadb/bin/mysqldump -uroot -pfe2112zaxlw7oaumut6a --no-data --databases [dbname, dbname] > seededschemas.sql
	dbNamesSpaces := strings.Join(databaseNames, " ")
	os.Chdir("/var/vcap/store/")
	mysqlDumpCmd := exec.Command("bash", "-c", "/var/vcap/packages/mariadb/bin/mysqldump --defaults-file=/var/vcap/jobs/mysql/config/mylogin.cnf --no-data --skip-lock-tables --databases "+dbNamesSpaces+" > seededschemas.sql")
	out, err := mysqlDumpCmd.CombinedOutput()
	if err != nil {
		println(err.Error())
		println(string(out))
		panic(err)
	}

	mysqlLoadCmd := exec.Command("bash", "-c", "/var/vcap/packages/pxc/bin/mysql --defaults-file=/var/vcap/jobs/mysql-clustered/config/mylogin.cnf < seededschemas.sql")
	out, err = mysqlLoadCmd.CombinedOutput()
	if err != nil {
		println(err.Error())
		println(string(out))
		panic(err)
	}

	// Update row formats to mariadb default innodb format
	query = "select CONCAT('ALTER TABLE ',table_schema,'.',table_name,' ROW_FORMAT=compact') from information_schema.tables where table_schema NOT IN ('performance_schema','mysql','information_schema','sys');"
	rows, err = pxcDatabaseConnection.Query(query)
	if err != nil {
		panic(err)
	}
	var rowFormatQueries []string
	for rows.Next() {
		var rowFormatQuery string
		rows.Scan(&rowFormatQuery)
		rowFormatQueries = append(rowFormatQueries, rowFormatQuery)
	}
	for _, rowFormatStatement := range rowFormatQueries {
		_, err = pxcDatabaseConnection.Exec(rowFormatStatement)
		if err != nil {
			panic(err)
		}
	}

	// Discard Tablespaces
	query = "select CONCAT('ALTER TABLE ',table_schema,'.',table_name,' DISCARD TABLESPACE') from information_schema.tables where table_schema NOT IN ('performance_schema','mysql','information_schema','sys');"
	rows, err = pxcDatabaseConnection.Query(query)
	if err != nil {
		panic(err)
	}
	var discardTableSpaceQueries []string
	for rows.Next() {
		var discardTableSpaceQuery string
		rows.Scan(&discardTableSpaceQuery)
		discardTableSpaceQueries = append(discardTableSpaceQueries, discardTableSpaceQuery)
	}
	for _, discardTableSpaceStatement := range discardTableSpaceQueries {
		_, err = pxcDatabaseConnection.Exec(discardTableSpaceStatement)
		if err != nil {
			panic(err)
		}
	}

	// Copy all the .ibd and .cfg files from /var/vcap/store/mysql to /var/vcap/store/mysql-clustered
	os.Chdir("/var/vcap/store/mysql")
	rsyncCmd := exec.Command("bash", "-c", "rsync -av --exclude='mysql' --exclude='performance_schema' --include='**/*.ibd' --include='**/*.cfg' --include='*/' --exclude='*' . ../mysql-clustered")
	out, err = rsyncCmd.CombinedOutput()
	if err != nil {
		println(err.Error())
		println(string(out))
		panic(err)
	}

	// Import Tablespaces
	query = "select CONCAT('ALTER TABLE ',table_schema,'.',table_name,' IMPORT TABLESPACE') from information_schema.tables where table_schema NOT IN ('performance_schema', 'mysql', 'information_schema','sys');"
	rows, err = pxcDatabaseConnection.Query(query)
	if err != nil {
		panic(err)
	}
	var importTableSpaceQueries []string
	for rows.Next() {
		var importTableSpaceQuery string
		rows.Scan(&importTableSpaceQuery)
		importTableSpaceQueries = append(importTableSpaceQueries, importTableSpaceQuery)
	}
	for _, importTableSpaceStatement := range importTableSpaceQueries {
		_, err = pxcDatabaseConnection.Exec(importTableSpaceStatement)
		if err != nil {
			panic(err)
		}
	}

	// Update row formats to 'default' mysql 5.7
	query = "select CONCAT('ALTER TABLE ',table_schema,'.',table_name,' ROW_FORMAT=default') from information_schema.tables where table_schema NOT IN ('performance_schema', 'mysql', 'information_schema','sys');"
	rows, err = pxcDatabaseConnection.Query(query)
	if err != nil {
		panic(err)
	}

	rowFormatQueries = []string{}
	for rows.Next() {
		var rowFormatQuery string
		rows.Scan(&rowFormatQuery)
		rowFormatQueries = append(rowFormatQueries, rowFormatQuery)
	}
	for _, rowFormatStatement := range rowFormatQueries {
		_, err = pxcDatabaseConnection.Exec(rowFormatStatement)
		if err != nil {
			panic(err)
		}
	}

	mariadbShutdownCmd := exec.Command("/var/vcap/packages/mariadb/support-files/mysql.server", "stop", "--pid-file=/var/vcap/sys/run/mysql/mysql.pid")
	out, err = mariadbShutdownCmd.CombinedOutput()
	if err != nil {
		println(err.Error())
		println(string(out))
		panic(err)
	}

}
