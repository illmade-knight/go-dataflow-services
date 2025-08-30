//go:build integration

// Package e2e contains end-to-end tests for dataflow pipelines.
// This test file, enrichment_test.go, validates the dataflow from MQTT ingestion,
// through the cache-backed enrichment service, to a final Pub/Sub topic for verification.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-dataflow-services/pkg/enrich"
	"github.com/illmade-knight/go-dataflow-services/pkg/ingestion"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/auth"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/illmade-knight/go-test/loadgen"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const (
	generateEnrichedMessagesFor     = 2 * time.Second
	enrichmentE2ELoadTestNumDevices = 3
	enrichmentE2ELoadTestRate       = 1.0
)

func TestEnrichmentE2E(t *testing.T) {
	// --- Test & Logger Setup ---
	logger := zerolog.New(os.Stderr).
		With().Timestamp().Str("test", "TestEnrichmentE2E").Logger()

	projectID := auth.CheckGCPAuth(t)

	// --- Timing & Metrics Setup ---
	timings := make(map[string]string)
	testStart := time.Now()
	var publishedCount int
	var verifiedCount int
	var expectedMessageCount int
	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpected"] = strconv.Itoa(expectedMessageCount)
		timings["MessagesPublished(Actual)"] = strconv.Itoa(publishedCount)
		timings["MessagesVerified(Actual)"] = strconv.Itoa(verifiedCount)

		logger.Info().Msg("--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			logger.Info().Msgf("%-35s: %s", name, d)
		}
		logger.Info().Msg("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("enrichment-flow-%s", runID)
	ingestionTopicID := fmt.Sprintf("enrich-ingest-topic-%s", runID)
	enrichmentSubID := fmt.Sprintf("enrich-sub-%s", runID)
	enrichedTopicID := fmt.Sprintf("enrich-output-topic-%s", runID)
	verifierSubID := fmt.Sprintf("verifier-sub-%s", runID)
	//enrichmentService := fmt.Sprintf("enrichment-service-%s", runID)
	firestoreDatabase := fmt.Sprintf("devices-%s", runID)
	firestoreCollection := fmt.Sprintf("devices-e2e-%s", runID)
	logger.Info().Str("run_id", runID).Msg("Generated unique resources for test run")

	// 2. Build the services definition in memory.
	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e-enrichment",
			ProjectID: projectID,
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{CloudResource: servicemanager.CloudResource{Name: ingestionTopicID}},
						{CloudResource: servicemanager.CloudResource{Name: enrichedTopicID}}},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{CloudResource: servicemanager.CloudResource{Name: enrichmentSubID}, Topic: ingestionTopicID},
						{CloudResource: servicemanager.CloudResource{Name: verifierSubID}, Topic: enrichedTopicID},
					},
					FirestoreDatabases: []servicemanager.FirestoreDatabase{
						{
							CloudResource: servicemanager.CloudResource{Name: firestoreDatabase},
						},
					},
				},
			},
		},
	}

	// 3. Setup dependencies: Emulators and Real Firestore Client
	start := time.Now()
	mqttConn := emulators.SetupMosquittoContainer(t, totalTestContext, emulators.GetDefaultMqttImageContainer())
	redisConn := emulators.SetupRedisContainer(t, totalTestContext, emulators.GetDefaultRedisImageContainer())
	timings["EmulatorSetup"] = time.Since(start).String()

	fsClient, err := firestore.NewClient(totalTestContext, projectID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fsClient.Close() })

	psClient, err := pubsub.NewClient(totalTestContext, projectID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	// 4. Populate Firestore with enrichment data for all test devices.
	devices, deviceToClientID, cleanupFirestore := setupEnrichmentTestData(
		t,
		totalTestContext,
		fsClient,
		firestoreCollection,
		runID,
		enrichmentE2ELoadTestNumDevices,
		enrichmentE2ELoadTestRate,
	)
	t.Cleanup(cleanupFirestore)

	// 5. Start services and orchestrate resources.
	start = time.Now()
	directorService, directorURL := startServiceDirector(t, totalTestContext, logger, servicesConfig)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		directorService.Shutdown(shutdownCtx)
	})
	timings["ServiceStartup(Director)"] = time.Since(start).String()

	start = time.Now()
	err = directorService.SetupFoundationalDataflow(totalTestContext, dataflowName)
	require.NoError(t, err)
	timings["CloudResourceSetup(Director)"] = time.Since(start).String()

	t.Cleanup(func() {
		teardownStart := time.Now()
		teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer teardownCancel()
		err = directorService.TeardownDataflow(teardownCtx, dataflowName)
		require.NoError(t, err)
		timings["CloudResourceTeardown(Director)"] = time.Since(teardownStart).String()
	})

	qualifiedSubName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, verifierSubID)
	verifierSub := psClient.Subscriber(qualifiedSubName)

	start = time.Now()
	cfg := ingestion.LoadConfigDefaults(projectID)
	cfg.DataflowName = dataflowName
	cfg.ServiceDirectorURL = directorURL

	cfg.Routing = map[string]ingestion.RoutingConfig{
		"devflow-uplinks": {
			MqttTopic:            "devices/+/data",
			QoS:                  1,
			DestinationRouteName: "uplinks",
		},
	}
	cfg.Producers = map[string]ingestion.ProducerConfig{
		"uplinks": {OutputTopicID: ingestionTopicID},
	}

	cfg.MQTT.BrokerURL = mqttConn.EmulatorAddress

	ingestionLogger := logger.With().Str("service", "ingestion").Logger()
	ingestionSvc := startIngestionService(t, totalTestContext, ingestionLogger, cfg)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = ingestionSvc.Shutdown(shutdownCtx)
	})
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()

	start = time.Now()
	enrichCfg := enrich.LoadConfigDefaults(projectID) // Load defaults and override
	enrichCfg.DataflowName = dataflowName
	enrichCfg.ServiceDirectorURL = directorURL
	enrichCfg.OutputTopicID = enrichedTopicID
	enrichCfg.InputSubscriptionID = enrichmentSubID
	enrichCfg.CacheConfig.RedisConfig.Addr = redisConn.EmulatorAddress
	enrichCfg.CacheConfig.FirestoreConfig.CollectionName = firestoreCollection
	enrichmentSvc := startEnrichmentService(t, totalTestContext, logger, enrichCfg, fsClient)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = enrichmentSvc.Shutdown(shutdownCtx)
	})
	timings["ServiceStartup(Enrichment)"] = time.Since(start).String()
	logger.Info().Msg("All services started successfully.")

	// 6. Start the Pub/Sub verifier in the background.
	verificationDone := make(chan struct{})
	expectedCountCh := make(chan int, 1)

	// This validator now correctly checks the message structure for double-wrapping.
	enrichedMessageValidator := func(t *testing.T, msg *pubsub.Message) bool {
		var finalMsgData messagepipeline.MessageData
		if err := json.Unmarshal(msg.Data, &finalMsgData); err != nil {
			logger.Error().Err(err).Msg("Failed to unmarshal final message data for verification.")
			return false
		}

		// This is the critical check: ensure the inner payload is the original raw payload.
		var originalPayload TestPayload
		if err := json.Unmarshal(finalMsgData.Payload, &originalPayload); err != nil {
			logger.Error().Err(err).Str("payload", string(finalMsgData.Payload)).Msg("Final message's inner payload is not the expected raw format.")
			return false
		}

		// Now validate the enrichment data.
		if finalMsgData.EnrichmentData == nil {
			logger.Error().Msg("Enriched message missing EnrichmentData.")
			return false
		}
		uid, uidOk := finalMsgData.EnrichmentData["DeviceID"].(string)
		if !uidOk {
			logger.Error().Msg("Message is missing 'DeviceID' attribute for verification.")
			return false
		}
		expectedClientID, clientIDFound := deviceToClientID[uid]
		if !clientIDFound || expectedClientID == "" {
			logger.Error().Str("device_id", uid).Msg("DeviceID not found in expected map during verification.")
			return false
		}
		if name, ok := finalMsgData.EnrichmentData["name"].(string); !ok || name != expectedClientID {
			return false
		}
		if location, ok := finalMsgData.EnrichmentData["location"].(string); !ok || location == "" {
			return false
		}
		return true
	}

	go func() {
		defer close(verificationDone)
		verifiedCount = verifyPubSubMessages(t, logger, totalTestContext, verifierSub, expectedCountCh, enrichedMessageValidator)
	}()

	// 7. Run Load Generator
	generator := loadgen.NewLoadGenerator(loadgen.NewMqttClient(mqttConn.EmulatorAddress, "devices/+/data", 1, logger), devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateEnrichedMessagesFor)

	loadgenStart := time.Now()
	logger.Info().Int("expected_messages", expectedMessageCount).Msg("Starting MQTT load generator...")

	publishedCount, err = generator.Run(totalTestContext, generateEnrichedMessagesFor)
	require.NoError(t, err)

	expectedCountCh <- publishedCount
	close(expectedCountCh)

	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	// 8. Wait for verification to complete.
	logger.Info().Msg("Waiting for enrichment verification to complete...")
	select {
	case <-verificationDone:
		timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
		logger.Info().Msg("Verification successful!")
	case <-totalTestContext.Done():
		t.Fatal("Test timed out waiting for verification")
	}
}
