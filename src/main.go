package main

// @todo: Solve timestamp format
// @todo: Create a generic NDJSON parser

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/rjohnsen/logwarp/src/controllers"
	"github.com/rjohnsen/logwarp/src/parsers"
	"github.com/rjohnsen/logwarp/src/syskernel"
)

func main() {
	// Establish AppContext
	appContext, appContextErr := syskernel.NewAppContext("./settings.toml")

	if appContextErr != nil {
		log.Fatal("Error: %w", appContext)
		os.Exit(1)
	}

	// Handle incoming work orders
	_, natsSubsciptionErr := appContext.Nats.Subscribe("logwarp/commands", func(m *nats.Msg) {
		var message syskernel.NatsJobMessage

		err := json.Unmarshal(m.Data, &message)

		if err != nil {
			log.Fatal("Failed to parse log message: ", err)
		} else {
			fmt.Printf("ID: %s\n", message.Id)
			fmt.Printf("INDEX: %s\n", message.Index)

			log_exists, _ := controllers.LogFileExists(appContext.Settings.Logwarp.Logfolder, message.Log)

			if log_exists {
				logFile := filepath.Join(appContext.Settings.Logwarp.Logfolder, message.Log)

				// Determine which parser to run
				switch strings.ToLower(message.Parser) {
				case "grok":
					go parsers.GrokParser(appContext, message.Id, message.Index, logFile, message.Grok)
				default:
					fmt.Println("Nope. No such value")
				}
			} else {
				fmt.Println("Logfile %s does not exist", message.Log)
			}
		}
	})

	if natsSubsciptionErr != nil {
		log.Fatal(natsSubsciptionErr)
	}

	log.Println("Listening for log messages...")
	select {}
}

/*
	logs := []string{
		"./logs/01-L2-1.ndjson",
		"./logs/01-L2-2.ndjson",
		"./logs/01-L2-3.ndjson",
		"./logs/01-L2-4.ndjson",
		"./logs/01-L2-5.ndjson",
		"./logs/01-L2-6.ndjson",
	}

	for _, log := range logs {
		parsers.ElasticParser(client, "huntlab", log)
	}

*/
