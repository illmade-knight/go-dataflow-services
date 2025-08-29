//go:build integration

package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
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

	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)

	runID := uuid.NewString()
	projectID := "test-project"
	// REFACTOR: Define two separate output topics for routing.
	uplinkTopicID := "uplink-topic-" + runID
	uplinkSubID := "uplink-sub-" + runID
	joinTopicID := "join-topic-" + runID
	joinSubID := "join-sub-" + runID

	mqttConnection := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID))

	// BUG FIX: Initialize MQTT config from defaults to get timeouts etc.
	mqttBaseCfg := mqttconverter.LoadMQTTClientConfigFromEnv()
	mqttBaseCfg.BrokerURL = mqttConnection.EmulatorAddress

	// REFACTOR: Build the new, multi-route config from scratch.
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
		},
		BufferSize: 10,
		NumWorkers: 3,
		HTTPPort:   ":0", // Use a random available port for the test server.
		MQTT:       *mqttBaseCfg,
		Routing: map[string]RoutingConfig{
			"uplinks": {MqttTopic: "devices/+/up", QoS: 1},
			"joins":   {MqttTopic: "devices/+/join", QoS: 1},
		},
		Producers: map[string]ProducerConfig{
			"uplinks": {OutputTopicID: uplinkTopicID},
			"joins":   {OutputTopicID: joinTopicID},
		},
		PubsubOptions: pubsubConnection.ClientOptions,
	}

	// The enricher remains the same, adding deviceID and other metadata.
	ingestionEnricher := func(ctx context.Context, msg *messagepipeline.Message) (bool, error) {
		if msg.EnrichmentData == nil {
			msg.EnrichmentData = make(map[string]interface{})
		}
		msg.EnrichmentData["Topic"] = msg.Attributes["mqtt_topic"]
		msg.EnrichmentData["Timestamp"] = msg.PublishTime
		return false, nil
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

	subClient, err := pubsub.NewClient(ctx, cfg.BaseConfig.ProjectID, pubsubConnection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = subClient.Close() })

	// REFACTOR: Create resources for both verification subscriptions.
	createPubsubResources(t, ctx, subClient, projectID, uplinkTopicID, uplinkSubID)
	createPubsubResources(t, ctx, subClient, projectID, joinTopicID, joinSubID)

	uplinkSub := subClient.Subscriber(uplinkSubID)
	joinSub := subClient.Subscriber(joinSubID)

	t.Run("Publish to multiple topics and verify routing", func(t *testing.T) {
		require.Eventually(t, serviceWrapper.consumer.IsConnected, 10*time.Second, 100*time.Millisecond, "MQTT consumer did not connect")

		// --- Publish Messages ---
		uplinkPayload := map[string]interface{}{"value": 42}
		uplinkBytes, _ := json.Marshal(uplinkPayload)
		uplinkTopic := "devices/dev123/up"
		mqttTestPubClient.Publish(uplinkTopic, 1, false, uplinkBytes)

		joinPayload := map[string]interface{}{"appKey": "abc"}
		joinBytes, _ := json.Marshal(joinPayload)
		joinTopic := "devices/dev456/join"
		mqttTestPubClient.Publish(joinTopic, 1, false, joinBytes)

		// --- Verification ---
		var wg sync.WaitGroup
		wg.Add(2)

		var receivedUplink *messagepipeline.MessageData
		var receivedJoin *messagepipeline.MessageData

		// Goroutine to listen for the uplink message
		go func() {
			defer wg.Done()
			pullCtx, pullCancel := context.WithTimeout(ctx, 20*time.Second)
			defer pullCancel()
			err := uplinkSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
				msg.Ack()
				var result messagepipeline.MessageData
				if json.Unmarshal(msg.Data, &result) == nil {
					receivedUplink = &result
				}
				pullCancel()
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Receiving from uplink sub failed: %v", err)
			}
		}()

		// Goroutine to listen for the join message
		go func() {
			defer wg.Done()
			pullCtx, pullCancel := context.WithTimeout(ctx, 20*time.Second)
			defer pullCancel()
			err := joinSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
				msg.Ack()
				var result messagepipeline.MessageData
				if json.Unmarshal(msg.Data, &result) == nil {
					receivedJoin = &result
				}
				pullCancel()
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Receiving from join sub failed: %v", err)
			}
		}()

		wg.Wait()

		// Assertions for the uplink message
		require.NotNil(t, receivedUplink, "Did not receive the uplink message")
		assert.JSONEq(t, string(uplinkBytes), string(receivedUplink.Payload))
		require.NotNil(t, receivedUplink.EnrichmentData)
		assert.Equal(t, uplinkTopic, receivedUplink.EnrichmentData["Topic"])

		// Assertions for the join message
		require.NotNil(t, receivedJoin, "Did not receive the join message")
		assert.JSONEq(t, string(joinBytes), string(receivedJoin.Payload))
		require.NotNil(t, receivedJoin.EnrichmentData)
		assert.Equal(t, joinTopic, receivedJoin.EnrichmentData["Topic"])
	})
}
