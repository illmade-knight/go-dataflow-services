//go:build integration

package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createPubsubResources is a test helper that encapsulates the administrative
// task of creating and tearing down the Pub/Sub topic and subscription.
func createPubsubResources(t *testing.T, ctx context.Context, client *pubsub.Client, projectID, topicID, subID string) {
	t.Helper()
	topicAdmin := client.TopicAdminClient
	subAdmin := client.SubscriptionAdminClient

	topicName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	_, err := topicAdmin.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = topicAdmin.DeleteTopic(context.Background(), &pubsubpb.DeleteTopicRequest{Topic: topicName})
	})

	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
	_, err = subAdmin.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subName,
		Topic: topicName,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = subAdmin.DeleteSubscription(context.Background(), &pubsubpb.DeleteSubscriptionRequest{Subscription: subName})
	})
}

func TestIngestionServiceWrapper_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	deviceFinder := regexp.MustCompile(`^[^/]+/([^/]+)/[^/]+$`)
	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)

	// REFACTOR: Use unique names for test resources.
	runID := uuid.NewString()
	projectID := "test-project"
	outputTopicID := "ingestion-output-topic-" + runID
	verifierSubID := "ingestion-verifier-sub-" + runID

	mqttConnection := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	// REFACTOR: Use the updated GetDefaultPubsubConfig.
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID))

	cfg := LoadConfigDefaults(projectID)

	cfg.HTTPPort = ":0"
	cfg.MQTT.BrokerURL = mqttConnection.EmulatorAddress
	cfg.MQTT.Topic = "devices/+/data"
	cfg.OutputTopicID = outputTopicID
	cfg.HTTPPort = ":"
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

	// REFACTOR: Create the topic and subscription for the test.
	createPubsubResources(t, ctx, subClient, projectID, outputTopicID, verifierSubID)
	processedSub := subClient.Subscriber(verifierSubID)

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

		// REFACTOR: The context is always cancelled by the receive callback, so we only check for other errors.
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err, "Receiving from Pub/Sub failed")
		}
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
