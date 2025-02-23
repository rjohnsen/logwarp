package settings

import (
	"fmt"
	"io"
	"os"

	"github.com/pelletier/go-toml/v2"
)

// Settings holds the configuration parameters loaded from a TOML file.
type Settings struct {
	Address   string `toml:"address"`
	VerifyTLS bool   `toml:"verifytls"`
	Username  string `toml:"username"`
	Password  string `toml:"password"`
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
