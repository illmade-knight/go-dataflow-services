package bigqueries

import (
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/bqstore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"google.golang.org/api/option"
)

// Config holds all configuration for the BigQuery batch inserter application.
type Config struct {
	microservice.BaseConfig
	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string
	BigQueryConfig     bqstore.BigQueryDatasetConfig
	ClientConnections  map[string][]option.ClientOption

	InputSubscriptionID string
	// Embed the generic batching service config.
	BatchProcessing messagepipeline.BatchingServiceConfig
}

// LoadConfigDefaults initializes and loads configuration.
func LoadConfigDefaults(projectID string) *Config {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
		},
		BatchProcessing: messagepipeline.BatchingServiceConfig{
			NumWorkers:    5,
			BatchSize:     100,
			FlushInterval: 1 * time.Minute,
		},
	}

	return cfg
}
