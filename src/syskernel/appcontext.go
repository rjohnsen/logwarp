package syskernel

import (
	"crypto/tls"
	"net/http"

	"github.com/nats-io/nats.go"
	"github.com/opensearch-project/opensearch-go"
	"github.com/rjohnsen/logwarp/src/settings"
)

type AppContext struct {
	Settings   *settings.Settings
	Nats       *nats.Conn
	OpenSearch *opensearch.Client
}

func NewAppContext(settingsPath string) (*AppContext, error) {
	// Load settings
	systemSettings, sysErr := settings.LoadSettings(settingsPath)

	if sysErr != nil {
		return &AppContext{}, sysErr
	}

	// Load OpenSearch Client
	opsClient, opsErr := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: systemSettings.OpenSearch.VerifyTLS},
		},
		Addresses: []string{systemSettings.OpenSearch.Address},
		Username:  systemSettings.OpenSearch.Username,
		Password:  systemSettings.OpenSearch.Password,
	})

	if opsErr != nil {
		return &AppContext{}, sysErr
	}

	// Load NATS Client
	natsClient, natsErr := nats.Connect(systemSettings.Nats.Address)

	if natsErr != nil {
		return &AppContext{}, sysErr
	}

	return &AppContext{
		Settings:   &systemSettings,
		Nats:       natsClient,
		OpenSearch: opsClient,
	}, nil
}
