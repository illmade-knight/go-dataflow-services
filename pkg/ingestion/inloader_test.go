package ingestion_test

import (
	"os"
	"testing"

	"github.com/illmade-knight/go-dataflow-services/pkg/ingestion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validYAMLConfig now reflects the final structure of routes.yaml,
// mapping a descriptive route name to a source MQTT topic and a logical destination route name.
const validYAMLConfig = `
routes:
  uplink-mappings:
    source:
      mqtt_topic: "devices/+/up"
      qos: 1
    destination:
      route_name: "uplinks"
  join-mappings:
    source:
      mqtt_topic: "devices/+/join"
      qos: 2
    destination:
      route_name: "joins"
`

// multiDeviceYAMLConfig demonstrates mapping multiple specific MQTT topics
// to the same logical destination route.
const multiDeviceYAMLConfig = `
routes:
  device-01-uplink:
    source:
      mqtt_topic: "devices/dev01/up"
      qos: 1
    destination:
      route_name: "uplinks"
  device-02-uplink:
    source:
      mqtt_topic: "devices/dev02/up"
      qos: 1
    destination:
      route_name: "uplinks"
`

// duplicateYAMLConfig tests the loader's ability to detect duplicate top-level route keys.
const duplicateYAMLConfig = `
routes:
  uplink-mappings:
    source:
      mqtt_topic: "devices/+/up"
      qos: 1
    destination:
      route_name: "uplinks"
  uplink-mappings:
    source:
      mqtt_topic: "devices/+/other"
      qos: 1
    destination:
      route_name: "others"
`

const invalidYAMLConfig = `
routes:
  uplinks:
    source:
      mqtt_topic: "devices/+/up"
  - invalid-structure
`

func TestYAMLConfigLoader(t *testing.T) {
	t.Run("successfully loads valid config", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test-*.yaml")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.Remove(tmpFile.Name()) })

		_, err = tmpFile.WriteString(validYAMLConfig)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		cfg := &ingestion.Config{}
		destinationRouteNames, err := ingestion.LoadRoutesFromYAML(tmpFile.Name(), cfg)
		require.NoError(t, err)

		// Assert that the loader correctly parsed the file
		require.Len(t, cfg.Routing, 2, "Should have loaded 2 routing configs")
		require.Len(t, destinationRouteNames, 2, "Should have found 2 unique destination route names")
		assert.Contains(t, destinationRouteNames, "uplinks")
		assert.Contains(t, destinationRouteNames, "joins")

		// Verify uplink route details
		uplinkRoute, ok := cfg.Routing["uplink-mappings"]
		require.True(t, ok, "Uplink route config should exist")
		assert.Equal(t, "devices/+/up", uplinkRoute.MqttTopic)
		assert.Equal(t, byte(1), uplinkRoute.QoS)
		assert.Equal(t, "uplinks", uplinkRoute.DestinationRouteName, "Destination route name should be correctly parsed")

		// Verify join route details
		joinRoute, ok := cfg.Routing["join-mappings"]
		require.True(t, ok, "Join route config should exist")
		assert.Equal(t, "devices/+/join", joinRoute.MqttTopic)
		assert.Equal(t, byte(2), joinRoute.QoS)
		assert.Equal(t, "joins", joinRoute.DestinationRouteName, "Destination route name should be correctly parsed")
	})

	t.Run("successfully loads multiple mappings to one destination", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "multi-*.yaml")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.Remove(tmpFile.Name()) })

		_, err = tmpFile.WriteString(multiDeviceYAMLConfig)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		cfg := &ingestion.Config{}
		destinationRouteNames, err := ingestion.LoadRoutesFromYAML(tmpFile.Name(), cfg)
		require.NoError(t, err)

		// Assert that the loader correctly parsed the file
		require.Len(t, cfg.Routing, 2, "Should have loaded 2 routing configs")
		require.Len(t, destinationRouteNames, 1, "Should have found 1 unique destination route name")
		assert.Contains(t, destinationRouteNames, "uplinks", "The only destination should be 'uplinks'")

		// Verify both device routes point to the same destination
		dev1Route, ok := cfg.Routing["device-01-uplink"]
		require.True(t, ok)
		assert.Equal(t, "uplinks", dev1Route.DestinationRouteName)

		dev2Route, ok := cfg.Routing["device-02-uplink"]
		require.True(t, ok)
		assert.Equal(t, "uplinks", dev2Route.DestinationRouteName)
	})

	t.Run("validator passes for consistent config", func(t *testing.T) {
		producers := map[string]ingestion.ProducerConfig{
			"uplinks": {OutputTopicID: "topic-a"},
			"joins":   {OutputTopicID: "topic-b"},
		}
		destinationRouteNames := map[string]bool{
			"uplinks": true,
			"joins":   true,
		}
		err := ingestion.ValidateRoutingAndProducers(producers, destinationRouteNames)
		assert.NoError(t, err, "Validation should pass when producers and destinations match")
	})

	t.Run("validator fails for inconsistent config", func(t *testing.T) {
		producers := map[string]ingestion.ProducerConfig{
			"uplinks":   {OutputTopicID: "topic-a"},
			"joins":     {OutputTopicID: "topic-b"},
			"telemetry": {OutputTopicID: "topic-c"}, // This producer has no source route
		}
		destinationRouteNames := map[string]bool{
			"uplinks": true,
			"joins":   true,
		}
		err := ingestion.ValidateRoutingAndProducers(producers, destinationRouteNames)
		assert.Error(t, err, "Validation should fail when a producer has no corresponding destination")
		assert.Contains(t, err.Error(), "telemetry", "Error message should mention the misconfigured route")
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		cfg := &ingestion.Config{}
		_, err := ingestion.LoadRoutesFromYAML("/path/to/non/existent/file.yaml", cfg)
		require.Error(t, err)
	})

	t.Run("returns error for invalid YAML", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "invalid-*.yaml")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.Remove(tmpFile.Name()) })

		_, err = tmpFile.WriteString(invalidYAMLConfig)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		cfg := &ingestion.Config{}
		_, err = ingestion.LoadRoutesFromYAML(tmpFile.Name(), cfg)
		require.Error(t, err)
	})

	t.Run("returns error for duplicate route names", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "duplicate-*.yaml")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.Remove(tmpFile.Name()) })

		_, err = tmpFile.WriteString(duplicateYAMLConfig)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		cfg := &ingestion.Config{}
		_, err = ingestion.LoadRoutesFromYAML(tmpFile.Name(), cfg)
		require.Error(t, err)
		// BUG FIX: The YAML parser fails first with a more specific error.
		// Test against the actual error message for accuracy.
		assert.Contains(t, err.Error(), "already defined", "Should detect duplicate top-level keys")
	})
}
