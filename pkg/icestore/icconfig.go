package icestore

import (
	"flag"
	"github.com/illmade-knight/go-dataflow-services/pkg"
	"os"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"google.golang.org/api/option"
)

// Consumer defines the configuration for the Pub/Sub subscriber.
type Consumer struct {
	SubscriptionID  string
	CredentialsFile string
}

// IceStore defines configuration specific to Google Cloud Storage (GCS).
type IceStore struct {
	CredentialsFile string
	BucketName      string
	ObjectPrefix    string
}

// Config holds all configuration for the IceStore microservice.
type Config struct {
	microservice.BaseConfig
	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string
	Consumer           Consumer
	IceStore           IceStore
	PubsubOptions      []option.ClientOption
	GCSOptions         []option.ClientOption

	// Use the single, correct config from the messagepipeline.
	BatchProcessing messagepipeline.BatchingServiceConfig
}

// LoadConfigDefaults initializes and loads configuration.
func LoadConfigDefaults(projectID string) (*Config, error) {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
			HTTPPort:  ":8083",
		},
	}
	cfg.BatchProcessing.NumWorkers = 5
	cfg.BatchProcessing.BatchSize = 100
	cfg.BatchProcessing.FlushInterval = 1 * time.Minute

	flag.IntVar(&cfg.BatchProcessing.NumWorkers, "batch.num-workers", cfg.BatchProcessing.NumWorkers, "Number of processing workers")
	flag.IntVar(&cfg.BatchProcessing.BatchSize, "batch.size", cfg.BatchProcessing.BatchSize, "GCS batch write size")
	flag.DurationVar(&cfg.BatchProcessing.FlushInterval, "batch.flush-interval", cfg.BatchProcessing.FlushInterval, "GCS batch flush interval")
	flag.Parse()

	pkg.OverrideWithIntEnvVar("APP_BATCH_NUM_WORKERS", &cfg.BatchProcessing.NumWorkers)
	pkg.OverrideWithIntEnvVar("APP_BATCH_SIZE", &cfg.BatchProcessing.BatchSize)
	pkg.OverrideWithDurationEnvVar("APP_BATCH_FLUSH_INTERVAL", &cfg.BatchProcessing.FlushInterval)

	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	return cfg, nil
}
