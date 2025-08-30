//go:build integration

// Package e2e contains end-to-end tests for dataflow pipelines.
// This test file, devflow_test.go, validates a basic ingestion dataflow:
// MQTT -> Ingestion Service -> Pub/Sub.
// It serves as a simple health check for the ingestion service.
package e2e

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-dataflow-services/pkg/ingestion"
	"github.com/illmade-knight/go-test/auth"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/illmade-knight/go-test/loadgen"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

const (
	totalDevflowTestDuration = 100 * time.Second
	loadTestDuration         = 5 * time.Second
	loadTestNumDevices       = 5
	loadTestRate             = 2.0
)

func TestDevflowE2E(t *testing.T) {

	totalTestContext, cancel := context.WithTimeout(context.Background(), totalDevflowTestDuration)
	t.Cleanup(cancel)

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

		t.Log("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			t.Logf("%-35s: %s", name, d)
		}
		t.Log("------------------------------------")
	})

	logger := log.With().Str("test", "TestDevflowE2E").Logger()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("dev-flow-%s", runID)
	verifyTopicID := fmt.Sprintf("dev-ingestion-topic-%s", runID)
	verifySubscriptionID := fmt.Sprintf("dev-verifier-sub-%s", runID)
	logger.Info().Str("run_id", runID).Str("topic_id", verifyTopicID).Msg("Generated unique resources for test run")

	client, err := pubsub.NewClient(totalTestContext, projectID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	// 2. Build the services definition in memory.
	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e-devflow",
			ProjectID: projectID,
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{CloudResource: servicemanager.CloudResource{Name: verifyTopicID}},
					},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{CloudResource: servicemanager.CloudResource{Name: verifySubscriptionID}, Topic: verifyTopicID},
					},
				},
			},
		},
	}

	// 3. Start MQTT Broker and ServiceDirector
	start := time.Now()
	mqttContainer := emulators.SetupMosquittoContainer(t, totalTestContext, emulators.GetDefaultMqttImageContainer())
	timings["EmulatorSetup(MQTT)"] = time.Since(start).String()

	start = time.Now()
	directorLogger := logger.With().Str("service", "servicedirector").Logger()
	directorService, directorURL := startServiceDirector(t, totalTestContext, directorLogger, servicesConfig)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		directorService.Shutdown(shutdownCtx)
	})
	timings["ServiceStartup(Director)"] = time.Since(start).String()
	logger.Info().Str("url", directorURL).Msg("ServiceDirector is healthy")

	// 4. Set up resources via the director's API
	start = time.Now()

	err = directorService.SetupFoundationalDataflow(totalTestContext, dataflowName)
	require.NoError(t, err)

	timings["CloudResourceSetup(Director)"] = time.Since(start).String()
	logger.Info().Msg("ServiceDirector confirmed resource setup is complete.")

	t.Cleanup(func() {
		teardownStart := time.Now()
		logger.Info().Msg("Test finished. Requesting resource teardown from ServiceDirector...")
		teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer teardownCancel()
		err = directorService.TeardownDataflow(teardownCtx, dataflowName)
		require.NoError(t, err)
		timings["CloudResourceTeardown(Director)"] = time.Since(teardownStart).String()
	})

	// 5. Start Ingestion Service
	start = time.Now()
	ingestionLogger := logger.With().Str("source", "devflow").Logger()
	cfg := ingestion.LoadConfigDefaults(projectID)
	cfg.DataflowName = dataflowName
	cfg.ServiceDirectorURL = directorURL
	cfg.ProjectID = projectID

	// REFACTOR: This demonstrates the in-memory configuration pattern.
	// The key is to correctly set the `DestinationRouteName` to match the
	// key used in the `Producers` map below.
	cfg.Routing = map[string]ingestion.RoutingConfig{
		"devflow-uplinks": {
			MqttTopic:            "devices/+/data",
			QoS:                  1,
			DestinationRouteName: "uplinks",
		},
	}
	cfg.Producers = map[string]ingestion.ProducerConfig{
		"uplinks": {OutputTopicID: verifyTopicID},
	}

	// Validate the consistency of the in-memory configuration.
	destinationRouteNames := make(map[string]bool)
	for _, route := range cfg.Routing {
		destinationRouteNames[route.DestinationRouteName] = true
	}
	err = ingestion.ValidateRoutingAndProducers(cfg.Producers, destinationRouteNames)
	require.NoError(t, err)

	cfg.MQTT.BrokerURL = mqttContainer.EmulatorAddress
	ingestionService := startIngestionService(t, totalTestContext, ingestionLogger, cfg)
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = ingestionService.Shutdown(shutdownCtx)
	})
	logger.Info().Msg("Ingestion service started successfully.")

	// 6. Start the Pub/Sub verifier in the background.
	verificationDone := make(chan struct{})
	expectedCountCh := make(chan int, 1)

	qualifiedSubName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, verifySubscriptionID)
	sub := client.Subscriber(qualifiedSubName)

	go func() {
		defer close(verificationDone)
		var noValidationNeeded MessageValidationFunc = nil
		verifiedCount = verifyPubSubMessages(
			t,
			logger,
			totalTestContext,
			sub,
			expectedCountCh,
			noValidationNeeded,
		)
	}()

	// 7. Run Load Generator
	loadgenStart := time.Now()
	logger.Info().Msg("Starting MQTT load generator...")
	loadgenClient := loadgen.NewMqttClient(mqttContainer.EmulatorAddress, "devices/+/data", 1, logger)
	devices := make([]*loadgen.Device, loadTestNumDevices)
	for i := 0; i < loadTestNumDevices; i++ {
		devices[i] = &loadgen.Device{ID: fmt.Sprintf("e2e-device-%d-%s", i, runID), MessageRate: loadTestRate, PayloadGenerator: &testPayloadGenerator{}}
	}
	generator := loadgen.NewLoadGenerator(loadgenClient, devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(loadTestDuration)

	publishedCount, err = generator.Run(totalTestContext, loadTestDuration)
	require.NoError(t, err)
	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	// Send the exact count to the waiting verifier.
	expectedCountCh <- publishedCount
	close(expectedCountCh)

	// 8. Wait for verification to complete
	logger.Info().Msg("Waiting for Pub/Sub verification to complete...")
	select {
	case <-verificationDone:
		timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
		logger.Info().Msg("Verification successful!")
	case <-totalTestContext.Done():
		t.Fatal("Test timed out waiting for verification")
	}
}
