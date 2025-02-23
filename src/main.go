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
			fmt.Println(client.Info())
			// parsers.LineParser(client, "./logs/small.ndjson")
			parsers.LineParser(client, "./logs/01-L2-1.ndjson")

		}
	}
}
