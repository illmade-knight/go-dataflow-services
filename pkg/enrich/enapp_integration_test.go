//go:build integration

package enrich_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow-services/pkg/enrich"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
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

// receiveSingleMessage is a helper to pull a single message from a subscription for verification.
func receiveSingleMessage(t *testing.T, ctx context.Context, sub *pubsub.Subscriber, timeout time.Duration) *pubsub.Message {
	t.Helper()
	pullCtx, pullCancel := context.WithTimeout(ctx, timeout)
	defer pullCancel()

	var receivedMsg *pubsub.Message
	err := sub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
		msg.Ack()
		receivedMsg = msg
		pullCancel() // Stop receiving after the first message.
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		require.NoError(t, err, "Receiving from Pub/Sub failed")
	}
	return receivedMsg
}

func TestEnrichmentServiceWrapper_Integration(t *testing.T) {
	testContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.New(os.Stderr).Level(zerolog.DebugLevel)
	projectID := "test-project"

	// --- 1. Setup Emulators ---
	rc := emulators.GetDefaultRedisImageContainer()
	redisConn := emulators.SetupRedisContainer(t, testContext, rc)
	fc := emulators.GetDefaultFirestoreConfig(projectID)
	firestoreConn := emulators.SetupFirestoreEmulator(t, testContext, fc)
	// REFACTOR: Use the updated GetDefaultPubsubConfig.
	pc := emulators.GetDefaultPubsubConfig(projectID)
	pubsubConn := emulators.SetupPubsubEmulator(t, testContext, pc)

	// REFACTOR: Use unique names for test resources.
	runID := uuid.New().String()
	inputTopicID := fmt.Sprintf("raw-messages-%s", runID)
	inputSubID := fmt.Sprintf("enrichment-sub-%s", runID)
	outputTopicID := fmt.Sprintf("enriched-messages-%s", runID)

	// --- 2. Seed Firestore with Test Data ---
	fsClient, err := firestore.NewClient(testContext, projectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fsClient.Close() })

	testDeviceID := "device-001"
	testDeviceData := DeviceInfo{ClientID: "client-abc", LocationID: "location-123", Category: "sensor"}
	_, err = fsClient.Collection("devices").Doc(testDeviceID).Set(testContext, testDeviceData)
	require.NoError(t, err)

	// --- 3. Configure the Service Wrapper ---
	cfg := enrich.LoadConfigDefaults(projectID)
	cfg.HTTPPort = ":0"
	cfg.InputSubscriptionID = inputSubID
	cfg.OutputTopicID = outputTopicID
	cfg.CacheConfig.RedisConfig.Addr = redisConn.EmulatorAddress
	cfg.CacheConfig.FirestoreConfig.CollectionName = "devices"
	cfg.ClientConnections = map[string][]option.ClientOption{
		"pubsub": pubsubConn.ClientOptions,
	}

	// --- 4. Setup Pub/Sub Resources ---
	psClient, err := pubsub.NewClient(testContext, projectID, pubsubConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	// REFACTOR: Create test-specific pubsub resources.
	createPubsubResources(t, testContext, psClient, projectID, inputTopicID, inputSubID)
	createPubsubResources(t, testContext, psClient, projectID, outputTopicID, "verifier-sub-ok-"+runID)

	// --- 5. Assemble the Fetcher using the Decorator Pattern ---
	firestoreFetcher, err := cache.NewFirestore[string, DeviceInfo](testContext, cfg.CacheConfig.FirestoreConfig, fsClient, logger)
	require.NoError(t, err)
	redisFetcher, err := cache.NewRedisCache[string, DeviceInfo](testContext, &cfg.CacheConfig.RedisConfig, logger, firestoreFetcher)
	require.NoError(t, err)
	t.Cleanup(func() { _ = redisFetcher.Close() })

	// --- 6. Create and Start the Service Wrapper ---
	wrapper, err := enrich.NewEnrichmentServiceWrapper[string, DeviceInfo](testContext, cfg, logger, redisFetcher, BasicKeyExtractor, DeviceApplier)
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(testContext)
	t.Cleanup(serviceCancel)
	go func() {
		if startErr := wrapper.Start(serviceCtx); startErr != nil && !errors.Is(startErr, context.Canceled) {
			t.Logf("EnrichmentServiceWrapper.Start() failed: %v", startErr)
		}
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = wrapper.Shutdown(shutdownCtx)
	})

	// --- 7. Run Test ---
	t.Run("Successful Enrichment", func(t *testing.T) {
		verifierSub := psClient.Subscriber("verifier-sub-ok-" + runID)

		originalPayload := []byte(`{"value": 42}`)
		upstreamMessage := messagepipeline.MessageData{
			ID:      "test-upstream-id",
			Payload: originalPayload,
			EnrichmentData: map[string]interface{}{
				"DeviceID": testDeviceID,
			},
		}
		wrappedPayload, err := json.Marshal(upstreamMessage)
		require.NoError(t, err)

		// REFACTOR: Use the v2 publisher.
		publisher := psClient.Publisher(inputTopicID)
		defer publisher.Stop()
		res := publisher.Publish(testContext, &pubsub.Message{Data: wrappedPayload})
		_, err = res.Get(testContext)
		require.NoError(t, err)

		receivedMsg := receiveSingleMessage(t, testContext, verifierSub, 15*time.Second)
		require.NotNil(t, receivedMsg, "Did not receive an enriched message")

		var result messagepipeline.MessageData
		err = json.Unmarshal(receivedMsg.Data, &result)
		require.NoError(t, err)

		assert.JSONEq(t, string(originalPayload), string(result.Payload))
		require.NotNil(t, result.EnrichmentData)
		assert.Equal(t, testDeviceData.ClientID, result.EnrichmentData["name"])
		assert.Equal(t, testDeviceData.LocationID, result.EnrichmentData["location"])
		assert.Equal(t, testDeviceID, result.EnrichmentData["DeviceID"])
	})
}
