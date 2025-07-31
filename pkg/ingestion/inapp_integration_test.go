//go:build integration

package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestionServiceWrapper_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	deviceFinder := regexp.MustCompile(`^[^/]+/([^/]+)/[^/]+$`)
	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)

	mqttConnection := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig("test-project", map[string]string{"ingestion-output-topic": "ingestion-verifier-sub"}))

	projectID := "test-project"
	cfg := LoadConfigDefaults(projectID)

	cfg.HTTPPort = ":0"
	cfg.MQTT.BrokerURL = mqttConnection.EmulatorAddress
	cfg.MQTT.Topic = "devices/+/data"
	cfg.OutputTopicID = "ingestion-output-topic"
	//we don't need so many workers for our integration test
	cfg.NumWorkers = 3
	cfg.BufferSize = 10
	cfg.PubsubOptions = pubsubConnection.ClientOptions

	// The transformer is now a MessageEnricher: it modifies the message in-place.
	ingestionEnricher := func(ctx context.Context, msg *messagepipeline.Message) (bool, error) {
		topic := msg.Attributes["mqtt_topic"]
		var deviceID string

		matches := deviceFinder.FindStringSubmatch(topic)
		if len(matches) > 1 {
			deviceID = matches[1]
		}

		if msg.EnrichmentData == nil {
			msg.EnrichmentData = make(map[string]interface{})
		}
		msg.EnrichmentData["DeviceID"] = deviceID
		msg.EnrichmentData["Topic"] = topic
		msg.EnrichmentData["Timestamp"] = msg.PublishTime

		return false, nil // Do not skip, no error.
	}

	serviceWrapper, err := NewIngestionServiceWrapper(ctx, cfg, logger, ingestionEnricher)
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(ctx)
	t.Cleanup(serviceCancel)
	go func() {
		if startErr := serviceWrapper.Start(serviceCtx); startErr != nil && !errors.Is(startErr, context.Canceled) {
			t.Logf("IngestionServiceWrapper.Start() failed during test: %v", startErr)
		}
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = serviceWrapper.Shutdown(shutdownCtx)
	})

	mqttTestPubClient, err := emulators.CreateTestMqttPublisher(mqttConnection.EmulatorAddress, "test-publisher-main")
	require.NoError(t, err)
	t.Cleanup(func() { mqttTestPubClient.Disconnect(250) })

	subClient, err := pubsub.NewClient(ctx, cfg.ProjectID, pubsubConnection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = subClient.Close() })
	processedSub := subClient.Subscription("ingestion-verifier-sub")

	t.Run("Publish MQTT and Verify PubSub Output", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return serviceWrapper.consumer.IsConnected()
		}, 10*time.Second, 100*time.Millisecond, "MQTT consumer did not connect in time")

		devicePayload := map[string]interface{}{"temperature": 25.5, "humidity": 60}
		payloadBytes, err := json.Marshal(devicePayload)
		require.NoError(t, err)

		publishTopic := "devices/test-device-123/data"
		token := mqttTestPubClient.Publish(publishTopic, 1, false, payloadBytes)
		token.Wait()
		require.NoError(t, token.Error())

		pullCtx, pullCancel := context.WithTimeout(ctx, 30*time.Second)
		t.Cleanup(pullCancel)

		var receivedMsg *pubsub.Message
		err = processedSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
			msg.Ack()
			receivedMsg = msg
			pullCancel()
		})

		require.NoError(t, err, "Receiving from Pub/Sub should not fail")
		require.NotNil(t, receivedMsg, "Did not receive a message from Pub/Sub")

		var result messagepipeline.MessageData
		err = json.Unmarshal(receivedMsg.Data, &result)
		require.NoError(t, err)

		assert.JSONEq(t, string(payloadBytes), string(result.Payload))
		require.NotNil(t, result.EnrichmentData)
		assert.Equal(t, publishTopic, result.EnrichmentData["Topic"])
		assert.Equal(t, "test-device-123", result.EnrichmentData["DeviceID"])
	})
}
