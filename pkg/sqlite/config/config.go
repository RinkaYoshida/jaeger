package config

// Configuration describes the options to customize the storage behavior
type Configuration struct {
	DBPath string `yaml:"db-path"`
	CacheSize int `yaml:"cache-size"`
}