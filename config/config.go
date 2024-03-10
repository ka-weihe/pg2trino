package config

import "os"

// Config is a struct that holds the configuration for the application.
type Config struct {
	TrinoHost    string
	TrinoPort    string
	TrinoCatalog string
	TrinoSchema  string
}

// NewConfig returns a new Config struct.
func NewConfig() *Config {
	return &Config{
		TrinoHost:    getEnv("TRINO_HOST", "localhost"),
		TrinoPort:    getEnv("TRINO_PORT", "8080"),
		TrinoCatalog: getEnv("TRINO_CATALOG", "hive"),
		TrinoSchema:  getEnv("TRINO_SCHEMA", "default"),
	}
}

// getEnv returns the value of an environment variable or
// a default value if the environment variable is not set.
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
