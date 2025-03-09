package settings

import (
	"fmt"
	"io"
	"os"

	"github.com/pelletier/go-toml/v2"
)

type Nats struct {
	Address string `toml:"address"`
}

type Logwarp struct {
	Logfolder string `toml:"logfolder"`
}

type OpenSearch struct {
	Address   string `toml:"address"`
	VerifyTLS bool   `toml:"verifytls"`
	Username  string `toml:"username"`
	Password  string `toml:"password"`
	BulkSize  int    `toml:"bulksize"`
}

type Settings struct {
	Nats       Nats       `toml:"nats"`
	Logwarp    Logwarp    `toml:"logwarp"`
	OpenSearch OpenSearch `toml:opensearch`
}

// LoadSettings reads a TOML configuration file and unmarshals it into a Settings struct.
// It returns an error if the file cannot be opened, read, or parsed.
func LoadSettings(settingsPath string) (Settings, error) {
	var settings Settings

	// Open the settings file
	settingsFile, err := os.Open(settingsPath)
	if err != nil {
		return settings, fmt.Errorf("failed to open settings file '%s': %w", settingsPath, err)
	}
	defer settingsFile.Close()

	// Read the entire file content
	bytesContent, err := io.ReadAll(settingsFile)
	if err != nil {
		return settings, fmt.Errorf("failed to read settings file '%s': %w", settingsPath, err)
	}

	// Unmarshal TOML data into the Settings struct
	if err := toml.Unmarshal(bytesContent, &settings); err != nil {
		return settings, fmt.Errorf("failed to parse TOML settings file '%s': %w", settingsPath, err)
	}

	return settings, nil
}
