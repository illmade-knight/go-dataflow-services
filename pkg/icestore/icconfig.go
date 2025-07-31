package icestore

import (
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/icestore"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"google.golang.org/api/option"
)

// IceStore defines configuration specific to Google Cloud Storage (GCS).
type IceStore struct {
	CredentialsFile string
	BucketName      string
	ObjectPrefix    string
}

// Config holds all configuration for the IceStore microservice.
type Config struct {
	microservice.BaseConfig
	IceStore      IceStore
	PubsubOptions []option.ClientOption
	GCSOptions    []option.ClientOption

	InputSubscriptionID string
	// Use the specific config for the IceStorageService.
	ServiceConfig icestore.IceStorageServiceConfig
}

// LoadConfigDefaults initializes and loads configuration.
func LoadConfigDefaults(projectID string) *Config {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
		},
		ServiceConfig: icestore.IceStorageServiceConfig{
			NumWorkers:    5,
			BatchSize:     100,
			FlushInterval: time.Minute,
		},
	}

	return cfg
}
