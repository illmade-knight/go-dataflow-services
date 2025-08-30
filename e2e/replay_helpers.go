package e2e

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
	"google.golang.org/api/iterator"

	"github.com/illmade-knight/go-test/loadgen" // Import loadgen package
)

type rawFilePayload struct {
	ID                    string `json:"id"`
	OriginalPubsubPayload struct {
		DeviceID  string    `json:"device_id"`
		Timestamp time.Time `json:"timestamp"`
		Value     float64   `json:"value"`
	} `json:"original_pubsub_payload"`
	ArchivedAt time.Time `json:"archived_at"`
}

// ReadMessagesFromGCS reads gzipped JSON messages from a GCS bucket.
// It assumes the messages are in a format from which a deviceID can be extracted
// and groups them by device ID.
func ReadMessagesFromGCS(
	t *testing.T,
	ctx context.Context,
	logger zerolog.Logger,
	gcsClient *storage.Client,
	bucketName string,
) (map[string][][]byte, error) {
	t.Helper()
	deviceMessages := make(map[string][][]byte)
	replayLogger := logger.With().Str("component", "GCSReplayReader").Str("bucket", bucketName).Logger()

	bucket := gcsClient.Bucket(bucketName)

	it := bucket.Objects(ctx, nil) // Iterate through all objects in the bucket

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break // No more objects
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list GCS objects in bucket %s: %w", bucketName, err)
		}

		// Create a new reader for each object
		rc, err := bucket.Object(attrs.Name).NewReader(ctx)
		if err != nil {
			replayLogger.Warn().Err(err).Str("object", attrs.Name).Msg("Failed to create GCS object reader, skipping.")
			continue
		}

		gzr, err := gzip.NewReader(rc)
		if err != nil {
			_ = rc.Close() // Close the underlying reader if gzip fails
			replayLogger.Warn().Err(err).Str("object", attrs.Name).Msg("Failed to create gzip reader, skipping.")
			continue
		}

		scanner := bufio.NewScanner(gzr)
		for scanner.Scan() {
			rawPayload := scanner.Bytes()
			// To extract deviceID for grouping, we need to unmarshal just enough.
			var tempPayload rawFilePayload
			if err := json.Unmarshal(rawPayload, &tempPayload); err != nil {
				replayLogger.Warn().Err(err).Str("line", scanner.Text()).Msg("Failed to unmarshal JSON to get deviceID, skipping.")
				continue
			}

			if tempPayload.OriginalPubsubPayload.DeviceID == "" {
				replayLogger.Warn().Str("line", scanner.Text()).Msg("Payload missing deviceID, skipping.")
				continue
			}

			deviceMessages[tempPayload.OriginalPubsubPayload.DeviceID] = append(deviceMessages[tempPayload.OriginalPubsubPayload.DeviceID], rawPayload)
		}
		if err := scanner.Err(); err != nil {
			replayLogger.Warn().Err(err).Str("object", attrs.Name).Msg("Error reading GCS object with scanner.")
		}

		_ = gzr.Close() // Close gzip reader
		_ = rc.Close()  // Close object reader
	}
	replayLogger.Info().Int("num_devices", len(deviceMessages)).Msg("Finished reading messages from GCS.")
	return deviceMessages, nil
}

// CreateReplayDevices takes messages grouped by device ID and a replay duration,
// and returns a slice of loadgen.Device objects configured for replay.
func CreateReplayDevices(
	t *testing.T,
	logger zerolog.Logger,
	deviceMessages map[string][][]byte,
	replayDuration time.Duration,
) ([]*loadgen.Device, int) {
	t.Helper()
	replayLogger := logger.With().Str("component", "ReplayDeviceCreator").Logger()

	var devices []*loadgen.Device
	totalMessagesToReplay := 0

	if len(deviceMessages) == 0 {
		replayLogger.Warn().Msg("No messages provided to create replay devices.")
		return nil, 0
	}

	for deviceID, payloads := range deviceMessages {
		totalMessagesToReplay += len(payloads)
		// For each device, create a loadgen.Device with a ReplayPayloadGenerator
		// that will provide its specific set of messages.
		// The message rate is calculated to distribute the device's messages evenly over the replay duration.
		devices = append(devices, &loadgen.Device{
			ID:               deviceID,
			MessageRate:      float64(len(payloads)) / replayDuration.Seconds(),
			PayloadGenerator: loadgen.NewReplayPayloadGenerator(payloads), // Use loadgen's ReplayPayloadGenerator
		})
	}
	replayLogger.Info().Int("num_devices", len(devices)).Int("total_messages", totalMessagesToReplay).Msg("Created replay devices.")
	return devices, totalMessagesToReplay
}

// ReplayGCSMessagesToMQTT takes a slice of pre-configured loadgen.Device objects
// and publishes their messages to the MQTT emulator.
func ReplayGCSMessagesToMQTT(
	t *testing.T,
	ctx context.Context,
	logger zerolog.Logger,
	mqttEmulatorAddress string,
	devices []*loadgen.Device, // Now accepts []*loadgen.Device directly
	replayDuration time.Duration,
) (int, error) {
	t.Helper()
	replayLogger := logger.With().Str("component", "MQTTReplayer").Logger()

	if len(devices) == 0 {
		replayLogger.Warn().Msg("No replay devices provided. Skipping MQTT replay.")
		return 0, nil
	}

	loadgenClient := loadgen.NewMqttClient(mqttEmulatorAddress, "devices/+/data", 1, replayLogger)

	totalMessagesExpectedByLoadgen := 0
	for _, dev := range devices {
		// Calculate expected messages for each device based on its rate and the overall replay duration
		// This is important because the loadgen.Run uses this to determine how many messages to send
		// before it potentially times out or completes its internal ticker cycles.
		// Note: This might be slightly different from the exact count of messages in the ReplayPayloadGenerator
		// if the replayDuration doesn't perfectly align with the calculated rates.
		// For replay, the exact count from the generator is more critical.
		// However, LoadGenerator's ExpectedMessagesForDuration is useful for its internal logic.
		totalMessagesExpectedByLoadgen += int(dev.MessageRate * replayDuration.Seconds())
	}

	replayLogger.Info().Int("total_messages_configured", totalMessagesExpectedByLoadgen).Dur("duration", replayDuration).Msg("Starting MQTT replay...")
	generator := loadgen.NewLoadGenerator(loadgenClient, devices, replayLogger)
	publishedCount, err := generator.Run(ctx, replayDuration)
	if err != nil {
		return 0, fmt.Errorf("failed to run load generator for replay: %w", err)
	}
	replayLogger.Info().Int("replayed_count", publishedCount).Msg("MQTT replay finished.")
	return publishedCount, nil
}
