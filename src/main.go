package main

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/opensearch-project/opensearch-go"
	"github.com/rjohnsen/logwarp/src/parsers"
	"github.com/rjohnsen/logwarp/src/settings"
)

func main() {
	// Load settings
	settings, err := settings.LoadSettings("./settings.toml")

	if err != nil {
		fmt.Println(("Unable to load settings. Exiting."))
	} else {
		client, err := opensearch.NewClient(opensearch.Config{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: settings.VerifyTLS},
			},
			Addresses: []string{settings.Address},
			Username:  settings.Username,
			Password:  settings.Password,
		})

		if err != nil {
			fmt.Println("Error: %w", err)
		} else {
			fmt.Println("=== Client information ===")
			fmt.Println(client.Info())

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
					parsers.LineParser(client, "huntlab", log)
				}

			*/

			parsers.GrokParser(
				client,
				"apache",
				"./logs/apache_logs",
				"%{IPORHOST:clientip} %{USER:ident} %{USER:auth} \\[%{HTTPDATE:timestamp}\\] \"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})\" %{NUMBER:response} (?:%{NUMBER:bytes}|-) %{QS:referrer} %{QS:agent}",
			)
		}
	}
}
