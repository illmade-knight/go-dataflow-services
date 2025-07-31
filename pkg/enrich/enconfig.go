package enrich

import (
	"time"

	"google.golang.org/api/option"

	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
)

// CacheConfig defines settings for metadata caching.
type CacheConfig struct {
	RedisConfig       cache.RedisConfig
	FirestoreConfig   *cache.FirestoreConfig
	CacheWriteTimeout time.Duration
}

// Config holds all configuration for the enrichment microservice.
type Config struct {
	microservice.BaseConfig
	InputSubscriptionID string
	OutputTopicID       string
	CacheConfig         CacheConfig
	NumWorkers          int
	ClientConnections   map[string][]option.ClientOption
}

// LoadConfigDefaults initializes and loads configuration.
func LoadConfigDefaults(projectID string) *Config {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
			HTTPPort:  ":8082",
		},
		CacheConfig: CacheConfig{
			CacheWriteTimeout: 5 * time.Second,
			RedisConfig: cache.RedisConfig{
				CacheTTL: 2 * time.Hour,
			},
			FirestoreConfig: &cache.FirestoreConfig{
				ProjectID: projectID,
			},
		},
		NumWorkers: 5,
	}

	return cfg
}
