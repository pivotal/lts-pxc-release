package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	fmt.Println("It works!")

	db, err := sql.Open("mysql", "diego:1dcoqrm6gpuoyqzr7fms@tcp(sql-db.service.cf.internal:3306)/diego")
	if err != nil {
		fmt.Println("Failed to open db connection.")
		os.Exit(1)
	}

	err = db.Ping()
	if err != nil {
		fmt.Println("Can't ping the db.")
		os.Exit(1)
	}
	fmt.Println("Was able to ping the db.")

	defer db.Close()
	db.SetMaxOpenConns(300)
	db.SetMaxIdleConns(300)

	var wg sync.WaitGroup
	goroutines := 300
	wg.Add(goroutines)

	cmd := "SELECT SLEEP(?);"
	f := func() {
		defer wg.Done()

		res, err := db.Exec(cmd, 60)
		fmt.Println("Doing a new exec")
		if err != nil {
			fmt.Printf("Failed to exec '%s'.\n", cmd)
			os.Exit(1)
		}

		rows, _ := res.RowsAffected()
		fmt.Printf("Rows affected '%d'.\n", rows)
	}

	for i := 0; i < goroutines; i++ {
		go f()
	}

	fmt.Printf("Spun off %d goroutines.\n", goroutines)
	wg.Wait()
	fmt.Println("Done waiting for goroutines.")
}
