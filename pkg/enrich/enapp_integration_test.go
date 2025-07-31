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
	"cloud.google.com/go/pubsub"
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
	pc := emulators.GetDefaultPubsubConfig(projectID, nil)
	pubsubConn := emulators.SetupPubsubEmulator(t, testContext, pc)

	runID := uuid.New().String()[:8]
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

	inputTopic, err := psClient.CreateTopic(testContext, inputTopicID)
	require.NoError(t, err)
	_, err = psClient.CreateSubscription(testContext, inputSubID, pubsub.SubscriptionConfig{Topic: inputTopic})
	require.NoError(t, err)
	outputTopic, err := psClient.CreateTopic(testContext, outputTopicID)
	require.NoError(t, err)

	// --- 5. Assemble the Fetcher using the Decorator Pattern ---
	firestoreFetcher, err := cache.NewFirestore[string, DeviceInfo](testContext, cfg.CacheConfig.FirestoreConfig, fsClient, logger)
	require.NoError(t, err)
	redisFetcher, err := cache.NewRedisCache[string, DeviceInfo](testContext, &cfg.CacheConfig.RedisConfig, logger, firestoreFetcher)
	require.NoError(t, err)
	t.Cleanup(func() { _ = redisFetcher.Close() })

	// --- 6. Create and Start the Service Wrapper ---
	// The constructor internally creates the pipeline-aware composite enricher.
	// We only need to provide the main key extractor and applier functions.
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
		verifierSub, err := psClient.CreateSubscription(testContext, "verifier-sub-ok", pubsub.SubscriptionConfig{Topic: outputTopic})
		require.NoError(t, err)
		t.Cleanup(func() { _ = verifierSub.Delete(testContext) })

		// To test the service's default pipeline-aware behavior, we must send it a message
		// in the format it expects from an upstream service (like the ingestion service).
		originalPayload := []byte(`{"value": 42}`)
		upstreamMessage := messagepipeline.MessageData{
			ID:      "test-upstream-id",
			Payload: originalPayload,
			EnrichmentData: map[string]interface{}{
				"DeviceID": testDeviceID, // This is the key the robust BasicKeyExtractor will find.
			},
		}
		wrappedPayload, err := json.Marshal(upstreamMessage)
		require.NoError(t, err)

		// We publish the wrapped payload. The service is expected to unwrap it.
		res := inputTopic.Publish(testContext, &pubsub.Message{Data: wrappedPayload})
		_, err = res.Get(testContext)
		require.NoError(t, err)

		receivedMsg := receiveSingleMessage(t, testContext, verifierSub, 15*time.Second)
		require.NotNil(t, receivedMsg, "Did not receive an enriched message")

		var result messagepipeline.MessageData
		err = json.Unmarshal(receivedMsg.Data, &result)
		require.NoError(t, err)

		// Assert that the final payload is the original, unwrapped payload.
		assert.JSONEq(t, string(originalPayload), string(result.Payload))
		require.NotNil(t, result.EnrichmentData)
		// Assert that the NEW enrichment data has been added.
		assert.Equal(t, testDeviceData.ClientID, result.EnrichmentData["name"])
		assert.Equal(t, testDeviceData.LocationID, result.EnrichmentData["location"])
		// Assert that the OLD enrichment data is still present.
		assert.Equal(t, testDeviceID, result.EnrichmentData["DeviceID"])
	})
}
