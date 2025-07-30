package ingestion

import (
	"flag"
	"os"
	"strconv"

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
}

// LoadConfigDefaults initializes and loads configuration.
func LoadConfigDefaults(projectID string) (*Config, error) {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
			HTTPPort:  ":8081",
		},
		NumWorkers: 20,
	}

	mqttCfg := mqttconverter.LoadMQTTClientConfigFromEnv()
	cfg.MQTT = *mqttCfg

	flag.IntVar(&cfg.NumWorkers, "num-workers", cfg.NumWorkers, "Number of concurrent processing workers")
	flag.Parse()

	if workers := os.Getenv("INGESTION_NUM_WORKERS"); workers != "" {
		if val, err := strconv.Atoi(workers); err == nil && val > 0 {
			cfg.NumWorkers = val
		}
	}
	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}
	return cfg, nil
}
