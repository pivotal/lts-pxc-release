package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

var graLogDir = flag.String(
	"graLogDir",
	"",
	"Specifies the directory from which to purge GRA log files.",
)

var graLogDaysToKeep = flag.Int(
	"graLogDaysToKeep",
	60,
	"Specifies the maximum age of the GRA log files allowed.",
)

var pidfile = flag.String(
	"pidfile",
	"",
	"The location for the pidfile",
)

func main() {
	flag.Parse()

	// err := ioutil.WriteFile(*pidfile, []byte(strconv.Itoa(os.Getpid())), 0644)
	// if err != nil {
	// 	panic(err)
	// }

	dir, err := os.Open(*graLogDir)
	if err != nil {
		LogErrorWithTimestamp(err)
		os.Exit(1)
	}

	for {
		deleted, failed := findGraLogs(dir)
		LogWithTimestamp(fmt.Sprintf("Deleted %v files, failed to delete %v files\n", deleted, failed))
		LogWithTimestamp("Sleeping for one hour\n")
		time.Sleep(1 * time.Hour)
	}
}

func findGraLogs(dir *os.File) (int64, int64) {
	now := time.Now()
	deleteOlderThan := now.Add(-(time.Duration(*graLogDaysToKeep) * time.Hour * 24))

	var (
		deleted int64
		failed  int64
	)

	for {
		// files, err := dir.Readdir(1024)
		files, err := dir.Readdir(1024)
		if err != nil && err != io.EOF {
			LogErrorWithTimestamp(err)
		}

		if len(files) == 0 && err == io.EOF {
			break
		}

		for _, file := range files {
			if deleteOlderThan.Before(file.ModTime()) {
				// TODO don't delete all of the mysql data dir because that's crazy
				// TODO possibly find a way to glob with depth of 0 (is not recursive)
				// TODO rename file to main.go
				filePath := filepath.Clean(fmt.Sprintf("%s/%s", dir.Name(), file.Name()))
				err := os.Remove(filePath)
				if err != nil {
					failed++
					LogErrorWithTimestamp(err)
				} else {
					fmt.Printf("%s\n", filePath)
					deleted++
				}
			}
		}
	}

	return deleted, failed
}

// Runs command with stdout and stderr pipes connected to process
func runCommand(executable string, args ...string) (string, error) {
	cmd := exec.Command(executable, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), err
	}
	return string(out), nil
}

func LogWithTimestamp(format string, args ...interface{}) {
	fmt.Printf("[%s] - ", time.Now().Local())
	if nil == args {
		fmt.Printf(format)
	} else {
		fmt.Printf(format, args...)
	}
}

func LogErrorWithTimestamp(err error) {
	fmt.Fprintf(os.Stderr, "[%s] - ", time.Now().Local())
	fmt.Fprintf(os.Stderr, err.Error()+"\n")
}
