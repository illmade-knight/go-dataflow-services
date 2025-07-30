package bigqueries

import (
	"flag"
	"os"
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
func LoadConfigDefaults(projectID string) (*Config, error) {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
			HTTPPort:  ":8084",
		},
	}
	// Set defaults for the batching service.
	cfg.BatchProcessing.NumWorkers = 5
	cfg.BatchProcessing.BatchSize = 100
	cfg.BatchProcessing.FlushInterval = 1 * time.Minute

	flag.IntVar(&cfg.BatchProcessing.NumWorkers, "batch.num-workers", cfg.BatchProcessing.NumWorkers, "Number of processing workers")
	flag.IntVar(&cfg.BatchProcessing.BatchSize, "batch.size", cfg.BatchProcessing.BatchSize, "BigQuery batch insert size")
	flag.DurationVar(&cfg.BatchProcessing.FlushInterval, "batch.flush-interval", cfg.BatchProcessing.FlushInterval, "BigQuery batch flush interval")
	flag.Parse()

	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	return cfg, nil
}
