package ingestion

import (
	"google.golang.org/api/option"

	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
)

// Config is updated for the new architecture.
type Config struct {
	microservice.BaseConfig
	PubsubOptions []option.ClientOption

	MQTT          mqttconverter.MQTTClientConfig
	OutputTopicID string
	NumWorkers    int
	BufferSize    int
}

// LoadConfigDefaults initializes and loads configuration.
func LoadConfigDefaults(projectID string) *Config {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
			HTTPPort:  ":8081",
		},
		NumWorkers: 20,
		BufferSize: 1000,
	}

	mqttCfg := mqttconverter.LoadMQTTClientConfigFromEnv()
	cfg.MQTT = *mqttCfg

	return cfg
}
