//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/loadgen"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// TestPayload represents a simple, raw message payload from a device.
type TestPayload struct {
	DeviceID  string    `json:"device_id" bigquery:"device_id"`
	Timestamp time.Time `json:"timestamp" bigquery:"timestamp"`
	Value     float64   `json:"value" bigquery:"value"`
}

// EnrichedTestPayload represents the final data structure after enrichment, ready for BigQuery.
type EnrichedTestPayload struct {
	DeviceID   string    `bigquery:"device_id"`
	Timestamp  time.Time `bigquery:"timestamp"`
	Value      float64   `bigquery:"value"`
	ClientID   string    `bigquery:"client_id"`
	LocationID string    `bigquery:"location_id"`
	Category   string    `bigquery:"category"`
}

// DeviceInfo is the data structure for enrichment data stored in Firestore.
type DeviceInfo struct {
	ID         string
	Name       string
	ClientID   string
	LocationID string
	Category   string
}

// setupEnrichmentTestData creates devices and seeds Firestore for tests involving enrichment.
func setupEnrichmentTestData(
	t *testing.T,
	ctx context.Context,
	fsClient *firestore.Client,
	firestoreCollection string,
	runID string,
	numDevices int,
	rate float64,
) ([]*loadgen.Device, map[string]string, func()) {
	t.Helper()
	devices := make([]*loadgen.Device, numDevices)
	deviceToClientID := make(map[string]string)
	for i := 0; i < numDevices; i++ {
		deviceID := fmt.Sprintf("e2e-enrich-device-%d-%s", i, runID)
		clientID := fmt.Sprintf("client-for-%s", deviceID)
		deviceToClientID[deviceID] = clientID
		devices[i] = &loadgen.Device{ID: deviceID, MessageRate: rate, PayloadGenerator: &testPayloadGenerator{}}
		di := DeviceInfo{ID: deviceID, Name: fmt.Sprintf("enrich-device-%d", i), ClientID: clientID, LocationID: "loc-456", Category: "e2e-device"}
		_, err := fsClient.Collection(firestoreCollection).Doc(deviceID).Set(ctx, di)
		require.NoError(t, err, "Failed to set device document for %s", deviceID)
	}
	cleanupFunc := func() {
		log.Info().Msg("Cleaning up Firestore documents from helper...")
		for _, device := range devices {
			_, err := fsClient.Collection(firestoreCollection).Doc(device.ID).Delete(context.Background())
			if err != nil {
				log.Warn().Err(err).Str("device_id", device.ID).Msg("Failed to cleanup firestore doc")
			}
		}
	}
	return devices, deviceToClientID, cleanupFunc
}

var deviceFinder = regexp.MustCompile(`^[^/]+/([^/]+)/[^/]+$`)

var ingestionEnricher = func(ctx context.Context, msg *messagepipeline.Message) (bool, error) {
	topic, ok := msg.Attributes["mqtt_topic"]
	if !ok {
		return false, nil
	}
	var deviceID string
	if matches := deviceFinder.FindStringSubmatch(topic); len(matches) > 1 {
		deviceID = matches[1]
	}
	if msg.EnrichmentData == nil {
		msg.EnrichmentData = make(map[string]interface{})
	}
	msg.EnrichmentData["DeviceID"] = deviceID
	msg.EnrichmentData["Topic"] = topic
	msg.EnrichmentData["Timestamp"] = msg.PublishTime
	return false, nil
}

var bqTransformer = func(ctx context.Context, msg *messagepipeline.Message) (*TestPayload, bool, error) {
	var p TestPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal payload for BQ: %w", err)
	}
	return &p, false, nil
}

// bqEnrichedTransformer is now pipeline-aware. It unwraps the message from the
// enrichment service before transforming it into the final BigQuery schema.
var bqEnrichedTransformer = func(ctx context.Context, msg *messagepipeline.Message) (*EnrichedTestPayload, bool, error) {
	// Step 1: Unwrap the MessageData from the upstream (enrichment) service.
	var upstreamData messagepipeline.MessageData
	if err := json.Unmarshal(msg.Payload, &upstreamData); err != nil {
		return nil, false, fmt.Errorf("bqTransformer: failed to unwrap upstream MessageData: %w", err)
	}

	// Step 2: Unmarshal the inner, original payload.
	var p TestPayload
	if err := json.Unmarshal(upstreamData.Payload, &p); err != nil {
		return nil, false, fmt.Errorf("bqTransformer: failed to unmarshal inner TestPayload: %w", err)
	}

	// Step 3: Extract enrichment data from the unwrapped message.
	var locationID, category, clientID string
	if upstreamData.EnrichmentData != nil {
		locationID, _ = upstreamData.EnrichmentData["location"].(string)
		category, _ = upstreamData.EnrichmentData["serviceTag"].(string)
		clientID, _ = upstreamData.EnrichmentData["name"].(string)
	}

	// Step 4: Construct the final, flat record for BigQuery.
	return &EnrichedTestPayload{
		DeviceID:   p.DeviceID,
		Timestamp:  p.Timestamp,
		Value:      p.Value,
		ClientID:   clientID,
		LocationID: locationID,
		Category:   category,
	}, false, nil
}

func BasicKeyExtractor(msg *messagepipeline.Message) (string, bool) {
	if msg.EnrichmentData != nil {
		if deviceID, ok := msg.EnrichmentData["DeviceID"].(string); ok && deviceID != "" {
			return deviceID, true
		}
	}
	uid, ok := msg.Attributes["uid"]
	return uid, ok
}

func DeviceApplier(msg *messagepipeline.Message, data DeviceInfo) {
	if msg.EnrichmentData == nil {
		msg.EnrichmentData = make(map[string]interface{})
	}
	msg.EnrichmentData["name"] = data.ClientID
	msg.EnrichmentData["location"] = data.LocationID
	msg.EnrichmentData["serviceTag"] = data.Category
}

type testPayloadGenerator struct{}

func (g *testPayloadGenerator) GeneratePayload(device *loadgen.Device) ([]byte, error) {
	return json.Marshal(TestPayload{DeviceID: device.ID, Timestamp: time.Now().UTC(), Value: 123.45})
}
