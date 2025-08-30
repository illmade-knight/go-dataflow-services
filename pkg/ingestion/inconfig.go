package ingestion

import (
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"google.golang.org/api/option"

	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
)

// ProducerConfig defines the destination for a specific route.
type ProducerConfig struct {
	// OutputTopicID is the Google Pub/Sub topic to publish messages for this route to.
	OutputTopicID string `json:"outputTopicId"`
}

// RoutingConfig defines the source of a route.
type RoutingConfig struct {
	// MqttTopic is the MQTT topic filter to subscribe to (e.g., "devices/+/up").
	MqttTopic string `json:"mqttTopic"`
	// QoS is the quality of service for the MQTT subscription.
	QoS byte `json:"qos"`
	// REFACTOR: Add this field to hold the parsed destination name.
	// DestinationRouteName is the logical name of the producer this route maps to.
	DestinationRouteName string `json:"destinationRouteName"`
}

// Config is the main configuration structure for the ingestion service.
type Config struct {
	microservice.BaseConfig `json:"baseConfig"`
	BufferSize              int                            `json:"bufferSize"`
	NumWorkers              int                            `json:"numWorkers"`
	HTTPPort                string                         `json:"httpPort"`
	PubsubOptions           []option.ClientOption          `json:"-"` // Loaded programmatically
	MQTT                    mqttconverter.MQTTClientConfig `json:"mqtt"`

	// Routing maps a logical route name (e.g., "uplinks") to its source MQTT topic config.
	Routing map[string]RoutingConfig `json:"routing"`

	// Producers maps a logical route name to its destination Pub/Sub producer config.
	Producers map[string]ProducerConfig `json:"producers"`
}

// LoadConfigDefaults initializes and loads configuration.
func LoadConfigDefaults(projectID string) *Config {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
		},
		NumWorkers: 20,
		BufferSize: 1000,
	}

	mqttCfg := mqttconverter.LoadMQTTClientConfigWithEnv()
	cfg.MQTT = *mqttCfg

	return cfg
}
