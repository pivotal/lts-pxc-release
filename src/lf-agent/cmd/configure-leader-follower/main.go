package main

import (
	"log"
	"os"

	"lf-agent/cmd/configure-leader-follower/command"
	"lf-agent/config"
)

func main() {
	usage := "[USAGE] CONFIG_PATH=./path/to/config.yml ./configure-leader-follower <command> [configure-leader-follower|make-leader|make-read-only|inspect]"
	configPath := os.Getenv("CONFIG_PATH")

	if configPath == "" {
		log.Fatalf(usage)
	}

	cfg, err := config.NewConfig(configPath)
	if err != nil {
		log.Fatalf("Error in config: %s", err)
	}

	if len(os.Args) < 2 {
		log.Fatal(usage)
	}

	commandName := os.Args[1]

	command, ok := command.New(commandName, cfg)
	if !ok {
		log.Fatal(usage)
	}

	log.Printf("Started executing command: %s\n", commandName)

	if err := command.Execute(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Successfully executed command: %s\n", commandName)
	os.Exit(0)
}
