//go:build integration

// Package e2e contains end-to-end tests for dataflow pipelines.
// This test file, fullflowe2e_test.go, validates a complex fan-out dataflow:
// 1. MQTT -> Ingestion Service -> Pub/Sub (Topic A)
// 2. Pub/Sub (Topic A) -> Enrichment Service -> Pub/Sub (Topic B) -> BigQuery Sink
// 3. Pub/Sub (Topic A) -> IceStore Service (Archival) -> GCS Sink
package e2e

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-dataflow-services/pkg/enrich"
	"github.com/illmade-knight/go-dataflow-services/pkg/ingestion"
	"github.com/illmade-knight/go-test/auth"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/illmade-knight/go-test/loadgen"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	generateCombinedMessagesFor = 5 * time.Second
	combinedTestNumDevices      = 3
	combinedTestRate            = 1.0
)

var (
	keepResources = flag.Bool("keep-resources", false, "Set to true to keep cloud resources after the test for inspection.")
)

func TestEnrichmentBigQueryIceStoreE2E(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}

	logger := zerolog.New(os.Stderr).With().Timestamp().Str("test", "TestEnrichmentBigQueryIceStoreE2E").Logger()

	projectID := auth.CheckGCPAuth(t)

	timings := make(map[string]string)
	testStart := time.Now()
	var publishedCount, expectedMessageCount, verifiedBQCount, verifiedGCSCount int

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpected"] = strconv.Itoa(expectedMessageCount)
		timings["MessagesPublished(Actual)"] = strconv.Itoa(publishedCount)
		timings["MessagesVerified(BigQuery)"] = strconv.Itoa(verifiedBQCount)
		timings["MessagesVerified(GCS)"] = strconv.Itoa(verifiedGCSCount)

		logger.Info().Msg("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			logger.Info().Msgf("%-35s: %s", name, d)
		}
		logger.Info().Msg("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("enrich-bq-icestore-flow-%s", runID)
	ingestionTopicID := fmt.Sprintf("ingest-topic-%s", runID)
	enrichmentSubID := fmt.Sprintf("enrich-sub-%s", runID)
	enrichedTopicID := fmt.Sprintf("enriched-output-topic-%s", runID)
	bigquerySubID := fmt.Sprintf("bq-sub-%s", runID)
	icestoreSubID := fmt.Sprintf("icestore-sub-%s", runID)
	uniqueBucketName := fmt.Sprintf("sm-icestore-bucket-%s", runID)
	uniqueDatasetID := fmt.Sprintf("dev_enriched_dataset_%s", runID)
	uniqueTableID := fmt.Sprintf("dev_enriched_payloads_%s", runID)
	firestoreCollection := fmt.Sprintf("devices-e2e-%s", runID)

	enrichedSchemaIdentifier := "github.com/illmade-knight/go-dataflow-services/dataflow/devflow/e2e.EnrichedTestPayload"
	servicemanager.RegisterSchema(enrichedSchemaIdentifier, EnrichedTestPayload{})

	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{Name: "e2e-full-flow", ProjectID: projectID, Location: "US"},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{CloudResource: servicemanager.CloudResource{Name: ingestionTopicID}},
						{CloudResource: servicemanager.CloudResource{Name: enrichedTopicID}},
					},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{CloudResource: servicemanager.CloudResource{Name: enrichmentSubID}, Topic: ingestionTopicID},
						{CloudResource: servicemanager.CloudResource{Name: bigquerySubID}, Topic: enrichedTopicID},
						{CloudResource: servicemanager.CloudResource{Name: icestoreSubID}, Topic: ingestionTopicID},
					},
					GCSBuckets:       []servicemanager.GCSBucket{{CloudResource: servicemanager.CloudResource{Name: uniqueBucketName}, Location: "US"}},
					BigQueryDatasets: []servicemanager.BigQueryDataset{{CloudResource: servicemanager.CloudResource{Name: uniqueDatasetID}}},
					BigQueryTables: []servicemanager.BigQueryTable{{
						CloudResource: servicemanager.CloudResource{Name: uniqueTableID},
						Dataset:       uniqueDatasetID,
						SchemaType:    enrichedSchemaIdentifier,
					}},
				},
			},
		},
	}

	var opts []option.ClientOption
	if creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}
	start := time.Now()
	mqttConn := emulators.SetupMosquittoContainer(t, totalTestContext, emulators.GetDefaultMqttImageContainer())
	redisConn := emulators.SetupRedisContainer(t, totalTestContext, emulators.GetDefaultRedisImageContainer())
	timings["EmulatorSetup"] = time.Since(start).String()

	fsClient, err := firestore.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, fsClient.Close()) })

	bqClient, err := bigquery.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, bqClient.Close()) })

	gcsClient, err := storage.NewClient(totalTestContext, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, gcsClient.Close()) })

	psClient, err := pubsub.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	devices, deviceToClientID, cleanupFirestore := setupEnrichmentTestData(t, totalTestContext, fsClient, firestoreCollection, runID, combinedTestNumDevices, combinedTestRate)
	t.Cleanup(cleanupFirestore)

	directorService, directorURL := startServiceDirector(t, totalTestContext, logger, servicesConfig)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		directorService.Shutdown(shutdownCtx)
	})

	err = directorService.SetupFoundationalDataflow(totalTestContext, dataflowName)
	require.NoError(t, err)

	t.Cleanup(func() {
		if *keepResources && !t.Failed() {
			logger.Info().Msg("Test passed and -keep-resources flag is set. Cloud resources will be KEPT for inspection.")
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()

		// REFACTOR: The bucket must be emptied *before* we ask the ServiceManager to delete it.
		// This resolves the "bucket not empty" error.
		logger.Info().Str("bucket", uniqueBucketName).Msg("Cleanup: Forcefully emptying GCS bucket before teardown...")
		bucket := gcsClient.Bucket(uniqueBucketName)
		it := bucket.Objects(cleanupCtx, nil)
		for {
			attrs, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			// If we get an error listing objects, we should fail the cleanup.
			require.NoError(t, err, "failed to list objects during cleanup")

			if delErr := bucket.Object(attrs.Name).Delete(cleanupCtx); delErr != nil {
				// We can log this error but continue, as the final bucket delete will likely fail and provide a clearer error.
				logger.Warn().Err(delErr).Str("object", attrs.Name).Msg("Failed to delete GCS object during cleanup.")
			}
		}
		logger.Info().Str("bucket", uniqueBucketName).Msg("Cleanup: GCS bucket emptied.")

		// Now that the bucket is empty, the general teardown can proceed.
		err = directorService.TeardownDataflow(cleanupCtx, dataflowName)
		require.NoError(t, err)
	})

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

	enrichCfg := enrich.LoadConfigDefaults(projectID)
	enrichCfg.DataflowName = dataflowName
	enrichCfg.ServiceDirectorURL = directorURL
	enrichCfg.InputSubscriptionID = enrichmentSubID
	enrichCfg.OutputTopicID = enrichedTopicID
	enrichCfg.CacheConfig.RedisConfig.Addr = redisConn.EmulatorAddress
	enrichCfg.CacheConfig.FirestoreConfig.CollectionName = firestoreCollection
	enrichmentSvc := startEnrichmentService(t, totalTestContext, logger, enrichCfg, fsClient)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = enrichmentSvc.Shutdown(shutdownCtx)
	})

	bqSvc, err := startEnrichedBigQueryService(t, totalTestContext, logger, directorURL, projectID, bigquerySubID, uniqueDatasetID, uniqueTableID, dataflowName, bqEnrichedTransformer)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = bqSvc.Shutdown(shutdownCtx)
	})

	icestoreSvc := startIceStoreService(t, totalTestContext, logger, projectID, icestoreSubID, uniqueBucketName, dataflowName)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = icestoreSvc.Shutdown(shutdownCtx)
	})
	logger.Info().Msg("All services started successfully.")

	generator := loadgen.NewLoadGenerator(loadgen.NewMqttClient(mqttConn.EmulatorAddress, "devices/+/data", 1, logger), devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateCombinedMessagesFor)
	publishedCount, err = generator.Run(totalTestContext, generateCombinedMessagesFor)
	require.NoError(t, err)

	logger.Info().Msg("Starting BigQuery and GCS verification...")

	enrichedBigQueryValidator := func(t *testing.T, iter *bigquery.RowIterator) error {
		var rowCount int
		for {
			var row EnrichedTestPayload
			err := iter.Next(&row)
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to read BigQuery row: %w", err)
			}
			rowCount++
			_, clientIDFound := deviceToClientID[row.DeviceID]
			require.True(t, clientIDFound, "Device ID %s not found in expected map", row.DeviceID)
			require.NotEmpty(t, row.ClientID, "ClientID missing for device %s", row.DeviceID)
			require.Equal(t, "loc-456", row.LocationID, "LocationID mismatch for device %s", row.DeviceID)
		}
		require.Equal(t, publishedCount, rowCount, "the final number of rows in BigQuery should match the number of messages published")
		verifiedBQCount = rowCount
		return nil
	}
	verifyBigQueryRows(t, logger, totalTestContext, projectID, uniqueDatasetID, uniqueTableID, publishedCount, enrichedBigQueryValidator)

	verifyGCSResults(t, logger, totalTestContext, gcsClient, uniqueBucketName, publishedCount)
	verifiedGCSCount = publishedCount
}
