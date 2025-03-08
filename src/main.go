package main

// @todo: Solve timestamp format
// @todo: Create a generic NDJSON parser

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/opensearch-project/opensearch-go"
	"github.com/rjohnsen/logwarp/src/controllers"
	"github.com/rjohnsen/logwarp/src/parsers"
	"github.com/rjohnsen/logwarp/src/settings"
)

type LogWarpMessage struct {
	Index  string `json:"index"`
	Log    string `json:"log"`
	Grok   string `json:"grokpattern"`
	Parser string `json:"parser"`
}

func main() {
	// Load settings
	settings, err := settings.LoadSettings("./settings.toml")

	if err != nil {
		fmt.Println(("Unable to load settings. Exiting."))
	} else {
		/*
		 * OpenSearch client
		 */

		opseclient, err := opensearch.NewClient(opensearch.Config{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: settings.OpenSearch.VerifyTLS},
			},
			Addresses: []string{settings.OpenSearch.Address},
			Username:  settings.OpenSearch.Username,
			Password:  settings.OpenSearch.Password,
		})

		if err != nil {
			log.Fatal("Error: %w", err)
		}

		/*
		 * Connect to NATS server
		 */
		natsclient, err := nats.Connect(settings.Nats.Address)

		if err != nil {
			log.Fatal("Unable to connect to NATS: %v", err)
		}

		defer natsclient.Close()

		_, err = natsclient.Subscribe("logwarp/commands", func(m *nats.Msg) {
			var message LogWarpMessage

			err := json.Unmarshal(m.Data, &message)

			if err != nil {
				log.Println("Failed to marse log message: ", err)
			} else {
				fmt.Println(message.Index)
				fmt.Println(message.Log)
				fmt.Println(message.Grok)
				fmt.Println(message.Parser)

				log_exists, _ := controllers.LogFileExists(settings.Logwarp.Logfolder, message.Log)

				if log_exists {
					logFile := filepath.Join(settings.Logwarp.Logfolder, message.Log)

					// Determine which parser to run
					switch strings.ToLower(message.Parser) {
					case "grok":
						go parsers.GrokParser(opseclient, message.Index, logFile, message.Grok)
					default:
						fmt.Println("Nope. No such value")
					}
				} else {
					fmt.Println("Logfile %s does not exist", message.Log)
				}
			}
		})

		if err != nil {
			log.Fatal(err)
		}

		log.Println("Listening for log messages...")
		select {} // Keep the program running
	}
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
