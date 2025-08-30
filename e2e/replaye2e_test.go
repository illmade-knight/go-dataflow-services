//go:build integration

// Package e2e contains end-to-end tests for dataflow pipelines.
// This test file, replaye2e_test.go, validates a replay dataflow:
// 1. Reads archived data from a pre-existing GCS bucket.
// 2. Replays this data as new messages to an MQTT topic.
// 3. A new pipeline (Ingestion -> BigQuery) processes these replayed messages.
// 4. Verifies that the replayed data correctly lands in a new BigQuery table.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/illmade-knight/go-test/auth"
	"github.com/stretchr/testify/assert"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-dataflow-services/pkg/ingestion"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	replayToBigqueryMessagesFor = 5 * time.Second // Duration over which to replay messages
)

func TestReplayToSimpleBigqueryFlowE2E(t *testing.T) {
	// --- Logger and Prerequisite Checks ---
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("test", "TestReplayToSimpleBigqueryFlowE2E").Logger()

	projectID := auth.CheckGCPAuth(t)

	sourceGCSBucketName := os.Getenv("REPLAY_GCS_BUCKET_NAME")
	if sourceGCSBucketName == "" {
		t.Skip("Skipping replay test: Please set 'REPLAY_GCS_BUCKET_NAME' env var with a bucket containing archived messages.")
	}

	// --- Timing & Metrics Setup ---
	timings := make(map[string]string)
	testStart := time.Now()
	var replayedCount int
	var expectedReplayCount int

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpectedToReplay"] = strconv.Itoa(expectedReplayCount)
		timings["MessagesReplayed(Actual)"] = strconv.Itoa(replayedCount)
		timings["MessagesVerified(BigQuery)"] = strconv.Itoa(replayedCount)

		logger.Info().Msg("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			logger.Info().Msgf("%-35s: %s", name, d)
		}
		logger.Info().Msg("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	// 1. Define unique resources for this REPLAY run.
	runID := uuid.New().String()[:8]
	replayDataflowName := fmt.Sprintf("replay-bq-flow-%s", runID)
	replayIngestionTopicID := fmt.Sprintf("replay-ingest-topic-%s", runID)
	replayBigquerySubID := fmt.Sprintf("replay-bq-subscription-%s", runID)
	replayDatasetID := fmt.Sprintf("replay_dataset_%s", runID)
	replayTableID := fmt.Sprintf("replay_ingested_payloads_%s", runID)

	// 2. Build the services definition in memory for the target replay flow.
	schemaIdentifier := "github.com/illmade-knight/go-dataflow-services/dataflow/devflow/e2e.TestPayload"
	servicemanager.RegisterSchema(schemaIdentifier, TestPayload{})
	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{Name: "e2e-replay", ProjectID: projectID, Location: "US"},
		Dataflows: map[string]servicemanager.ResourceGroup{
			replayDataflowName: {
				Name:      replayDataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics:           []servicemanager.TopicConfig{{CloudResource: servicemanager.CloudResource{Name: replayIngestionTopicID}}},
					Subscriptions:    []servicemanager.SubscriptionConfig{{CloudResource: servicemanager.CloudResource{Name: replayBigquerySubID}, Topic: replayIngestionTopicID}},
					BigQueryDatasets: []servicemanager.BigQueryDataset{{CloudResource: servicemanager.CloudResource{Name: replayDatasetID}}},
					BigQueryTables:   []servicemanager.BigQueryTable{{CloudResource: servicemanager.CloudResource{Name: replayTableID}, Dataset: replayDatasetID, SchemaType: schemaIdentifier}},
				},
			},
		},
	}

	// 3. Setup dependencies
	var opts []option.ClientOption
	if creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}
	mqttConn := emulators.SetupMosquittoContainer(t, totalTestContext, emulators.GetDefaultMqttImageContainer())

	bqClient, err := bigquery.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, bqClient.Close()) })

	gcsClient, err := storage.NewClient(totalTestContext, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, gcsClient.Close()) })

	// 4. Read messages from the EXISTING GCS bucket.
	deviceMessagesToReplay, err := ReadMessagesFromGCS(t, totalTestContext, logger, gcsClient, sourceGCSBucketName)
	require.NoError(t, err)

	replayDevices, totalMessages := CreateReplayDevices(t, logger, deviceMessagesToReplay, replayToBigqueryMessagesFor)
	expectedReplayCount = totalMessages
	require.Greater(t, expectedReplayCount, 0, "No messages found in GCS bucket %s to replay.", sourceGCSBucketName)

	// 5. Start services for the new replay dataflow pipeline.
	directorService, directorURL := startServiceDirector(t, totalTestContext, logger.With().Str("service", "servicedirector").Logger(), servicesConfig)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		directorService.Shutdown(shutdownCtx)
	})

	err = directorService.SetupFoundationalDataflow(totalTestContext, replayDataflowName)
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		logger.Info().Msg("Requesting resource teardown from ServiceDirector for replay flow...")
		err = directorService.TeardownDataflow(cleanupCtx, replayDataflowName)
		require.NoError(t, err)
		ds := bqClient.Dataset(replayDatasetID)
		if err := ds.DeleteWithContents(cleanupCtx); err != nil {
			logger.Warn().Err(err).Str("dataset", replayDatasetID).Msg("Failed to delete BigQuery dataset during replay cleanup.")
		}
	})

	cfg := ingestion.LoadConfigDefaults(projectID)
	cfg.DataflowName = replayDataflowName
	cfg.ServiceDirectorURL = directorURL
	cfg.MQTT.BrokerURL = mqttConn.EmulatorAddress

	cfg.Routing = map[string]ingestion.RoutingConfig{
		"uplinks": {MqttTopic: "devices/+/data", QoS: 1},
	}
	cfg.Producers = map[string]ingestion.ProducerConfig{
		"uplinks": {OutputTopicID: replayIngestionTopicID},
	}

	ingestionLogger := logger.With().Str("service", "ingestion").Logger()
	ingestionSvc := startIngestionService(t, totalTestContext, ingestionLogger, cfg)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = ingestionSvc.Shutdown(shutdownCtx)
	})

	bqSvc := startBigQueryService(t, totalTestContext, logger, projectID, replayBigquerySubID, replayDatasetID, replayTableID, bqTransformer)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = bqSvc.Shutdown(shutdownCtx)
	})
	logger.Info().Msg("Replay target services started successfully.")

	// 6. Replay messages to MQTT emulator.
	replayStart := time.Now()
	logger.Info().Msg("Starting MQTT replay of GCS messages...")
	replayedCount, err = ReplayGCSMessagesToMQTT(t, totalTestContext, logger, mqttConn.EmulatorAddress, replayDevices, replayToBigqueryMessagesFor)
	require.NoError(t, err)
	timings["ReplayLoadGeneration"] = time.Since(replayStart).String()
	logger.Info().Int("replayed_count", replayedCount).Msg("Messages replayed to MQTT emulator.")

	// 7. Verify results in the NEW BigQuery table.
	verificationStart := time.Now()
	logger.Info().Msg("Starting BigQuery verification for replayed messages...")

	countValidator := func(t *testing.T, iter *bigquery.RowIterator) error {
		var rowCount int
		for {
			var row map[string]bigquery.Value
			err := iter.Next(&row)
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return err
			}
			rowCount++
		}
		require.Equal(t, replayedCount, rowCount, "the final number of rows in BigQuery should match the number of messages replayed")
		return nil
	}

	verifyBigQueryRows(t, logger, totalTestContext, projectID, replayDatasetID, replayTableID, replayedCount, countValidator)
	timings["VerificationDuration"] = time.Since(verificationStart).String()
}
