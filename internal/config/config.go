package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type Config struct {
	RedisAddresses []string `json:"redis_addresses"`
	RedisPassword  string   `json:"redis_password,omitempty"`
	RedisDB        int      `json:"redis_db"`
	PoolSize       int      `json:"pool_size"`
}

var (
	configPath string
	configMu   sync.RWMutex
)

func init() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "."
	}
	configDir := filepath.Join(homeDir, ".a2adb-tester")
	configPath = filepath.Join(configDir, "config.json")
}

// GetConfigPath returns the path to the config file
func GetConfigPath() string {
	return configPath
}

// Load reads the config from file, returns defaults if not found
func Load() (*Config, error) {
	configMu.RLock()
	defer configMu.RUnlock()

	cfg := &Config{
		RedisAddresses: []string{},
		RedisDB:        0,
		PoolSize:       500,
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Save writes the config to file
func Save(cfg *Config) error {
	configMu.Lock()
	defer configMu.Unlock()

	// Ensure directory exists
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(configPath, data, 0644)
}

// AddAddress adds a Redis address to the config
func AddAddress(address string) error {
	cfg, err := Load()
	if err != nil {
		return err
	}

	// Check if already exists
	for _, addr := range cfg.RedisAddresses {
		if addr == address {
			return nil
		}
	}

	cfg.RedisAddresses = append(cfg.RedisAddresses, address)
	return Save(cfg)
}

// RemoveAddress removes a Redis address from the config
func RemoveAddress(address string) error {
	cfg, err := Load()
	if err != nil {
		return err
	}

	newAddresses := make([]string, 0, len(cfg.RedisAddresses))
	for _, addr := range cfg.RedisAddresses {
		if addr != address {
			newAddresses = append(newAddresses, addr)
		}
	}

	cfg.RedisAddresses = newAddresses
	return Save(cfg)
}
