package ingestion

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// YAMLRouteConfig defines the structure for a single route in the YAML file.
type YAMLRouteConfig struct {
	Source struct {
		MqttTopic string `yaml:"mqtt_topic"`
		QoS       byte   `yaml:"qos"`
	} `yaml:"source"`
	Destination struct {
		// This is the logical name that must match a producer key.
		RouteName string `yaml:"route_name"`
	} `yaml:"destination"`
}

// YAMLShape is the top-level structure for parsing the routes YAML file.
type YAMLShape struct {
	Routes map[string]YAMLRouteConfig `yaml:"routes"`
}

// LoadRoutesFromYAML loads the MQTT topic-to-routename mappings from a YAML file.
// It populates the routing configuration and returns a set of all destination route names
// found in the file, which is used for validation.
func LoadRoutesFromYAML(filePath string, cfg *Config) (map[string]bool, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read route config file %s: %w", filePath, err)
	}

	var yamlCfg YAMLShape
	err = yaml.Unmarshal(data, &yamlCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse route config YAML from %s: %w", filePath, err)
	}

	if cfg.Routing == nil {
		cfg.Routing = make(map[string]RoutingConfig)
	}

	destinationRouteNames := make(map[string]bool)

	for name, route := range yamlCfg.Routes {
		if _, exists := cfg.Routing[name]; exists {
			return nil, fmt.Errorf("duplicate route name found in YAML config: %s", name)
		}
		// BUG FIX: The destination route name from the YAML must be stored in the config struct.
		cfg.Routing[name] = RoutingConfig{
			MqttTopic:            route.Source.MqttTopic,
			QoS:                  route.Source.QoS,
			DestinationRouteName: route.Destination.RouteName,
		}
		if route.Destination.RouteName != "" {
			destinationRouteNames[route.Destination.RouteName] = true
		}
	}
	return destinationRouteNames, nil
}

// ValidateRoutingAndProducers checks that the loaded configuration is consistent.
// It ensures that every logical route defined in the Producers map has at least
// one corresponding source MQTT topic that directs to it in the routes.yaml file.
// This prevents runtime errors from misconfigured routes where a producer has no data source.
func ValidateRoutingAndProducers(producers map[string]ProducerConfig, destinationRouteNames map[string]bool) error {
	for producerRouteName := range producers {
		if _, ok := destinationRouteNames[producerRouteName]; !ok {
			return fmt.Errorf("configuration validation failed: producer is defined for route '%s', but this route is not used as a destination in any mapping in routes.yaml", producerRouteName)
		}
	}
	return nil
}
