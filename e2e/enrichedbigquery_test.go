//go:build integration

// Package e2e contains end-to-end tests for dataflow pipelines.
// This test file, enrichedbigquery_test.go, validates the dataflow from
// MQTT ingestion, through the enrichment service, to a final BigQuery sink.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-dataflow-services/pkg/enrich"
	"github.com/illmade-knight/go-dataflow-services/pkg/ingestion"
	"github.com/illmade-knight/go-test/auth"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/illmade-knight/go-test/loadgen"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	generateEnrichedBigqueryMessagesFor = 5 * time.Second
	enrichmentBigQueryTestNumDevices    = 3
	enrichmentBigQueryTestRate          = 1.0
)

func TestEnrichmentToBigQueryE2E(t *testing.T) {
	logger := zerolog.New(os.Stderr).
		With().Timestamp().Str("test", "TestEnrichmentToBigQueryE2E").Logger()

	projectID := auth.CheckGCPAuth(t)
	// --- Timing & Metrics Setup ---
	timings := make(map[string]string)
	testStart := time.Now()
	var publishedCount int
	var expectedMessageCount int

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpected"] = strconv.Itoa(expectedMessageCount)
		timings["MessagesPublished(Actual)"] = strconv.Itoa(publishedCount)
		timings["MessagesVerified(Actual)"] = strconv.Itoa(publishedCount)

		t.Log("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			t.Logf("%-35s: %s", name, d)
		}
		t.Log("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("enrichment-bq-flow-%s", runID)
	ingestionTopicID := fmt.Sprintf("enrich-bq-ingest-topic-%s", runID)
	enrichmentSubID := fmt.Sprintf("enrich-bq-sub-%s", runID)
	enrichedTopicID := fmt.Sprintf("enrich-bq-output-topic-%s", runID)
	bigquerySubID := fmt.Sprintf("bq-sub-%s", runID)
	firestoreCollection := fmt.Sprintf("devices-e2e-%s", runID)
	uniqueDatasetID := fmt.Sprintf("dev_enriched_dataset_%s", runID)
	uniqueTableID := fmt.Sprintf("dev_enriched_payloads_%s", runID)

	// 2. Build the services definition in memory.
	schemaIdentifier := "github.com/illmade-knight/go-dataflow-services/dataflow/devflow/e2e.EnrichedTestPayload"
	servicemanager.RegisterSchema(schemaIdentifier, EnrichedTestPayload{})

	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e-enrichment",
			ProjectID: projectID,
			Location:  "US",
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics: []servicemanager.TopicConfig{{CloudResource: servicemanager.CloudResource{Name: ingestionTopicID}}, {CloudResource: servicemanager.CloudResource{Name: enrichedTopicID}}},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{CloudResource: servicemanager.CloudResource{Name: enrichmentSubID}, Topic: ingestionTopicID},
						{CloudResource: servicemanager.CloudResource{Name: bigquerySubID}, Topic: enrichedTopicID},
					},
					BigQueryDatasets: []servicemanager.BigQueryDataset{{CloudResource: servicemanager.CloudResource{Name: uniqueDatasetID}}},
					BigQueryTables: []servicemanager.BigQueryTable{
						{
							CloudResource:    servicemanager.CloudResource{Name: uniqueTableID},
							Dataset:          uniqueDatasetID,
							SchemaType:       schemaIdentifier,
							ClusteringFields: []string{"device_id"},
						},
					},
				},
			},
		},
	}

	// 3. Setup dependencies: Emulators and Real Clients
	var opts []option.ClientOption
	start := time.Now()
	mqttConn := emulators.SetupMosquittoContainer(t, totalTestContext, emulators.GetDefaultMqttImageContainer())
	redisConn := emulators.SetupRedisContainer(t, totalTestContext, emulators.GetDefaultRedisImageContainer())
	timings["EmulatorSetup"] = time.Since(start).String()

	fsClient, err := firestore.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fsClient.Close() })

	bqClient, err := bigquery.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bqClient.Close() })

	psClient, err := pubsub.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	// 4. Populate Firestore with enrichment data.
	devices, deviceToClientID, cleanupFirestore := setupEnrichmentTestData(
		t,
		totalTestContext,
		fsClient,
		firestoreCollection,
		runID,
		enrichmentBigQueryTestNumDevices,
		enrichmentBigQueryTestRate,
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
		teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer teardownCancel()

		err = directorService.TeardownDataflow(totalTestContext, dataflowName)
		if err != nil {
			logger.Warn().Msg("did not fully teardown dataflow")
		}

		ds := bqClient.Dataset(uniqueDatasetID)
		if err := ds.DeleteWithContents(teardownCtx); err != nil {
			logger.Warn().Err(err).Str("dataset", uniqueDatasetID).Msg("Failed to delete BigQuery dataset during cleanup.")
		}
		timings["CloudResourceTeardown(Director)"] = time.Since(teardownStart).String()
	})

	start = time.Now()
	cfg := ingestion.LoadConfigDefaults(projectID)
	cfg.DataflowName = dataflowName
	cfg.ServiceDirectorURL = directorURL
	cfg.MQTT.BrokerURL = mqttConn.EmulatorAddress

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

	ingestionLogger := logger.With().Str("service", "ingestion").Logger()
	ingestionSvc := startIngestionService(t, totalTestContext, ingestionLogger, cfg)
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = ingestionSvc.Shutdown(shutdownCtx)
	})

	start = time.Now()
	enrichCfg := enrich.LoadConfigDefaults(projectID) // Load defaults and override
	enrichCfg.DataflowName = dataflowName
	enrichCfg.ServiceDirectorURL = directorURL
	enrichCfg.OutputTopicID = enrichedTopicID
	enrichCfg.InputSubscriptionID = enrichmentSubID
	enrichCfg.CacheConfig.RedisConfig.Addr = redisConn.EmulatorAddress
	enrichCfg.CacheConfig.FirestoreConfig.CollectionName = firestoreCollection
	enrichmentSvc := startEnrichmentService(t, totalTestContext, logger, enrichCfg, fsClient)
	timings["ServiceStartup(Enrichment)"] = time.Since(start).String()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = enrichmentSvc.Shutdown(shutdownCtx)
	})

	start = time.Now()
	bqSvc, err := startEnrichedBigQueryService(t, totalTestContext, logger, directorURL, projectID, bigquerySubID, uniqueDatasetID, uniqueTableID, dataflowName, bqEnrichedTransformer)
	require.NoError(t, err)
	timings["ServiceStartup(BigQuery)"] = time.Since(start).String()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = bqSvc.Shutdown(shutdownCtx)
	})
	logger.Info().Msg("All services started successfully.")

	// 6. Run Load Generator
	loadgenStart := time.Now()
	logger.Info().Msg("Starting MQTT load generator...")
	generator := loadgen.NewLoadGenerator(loadgen.NewMqttClient(mqttConn.EmulatorAddress, "devices/+/data", 1, logger), devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateEnrichedBigqueryMessagesFor)

	publishedCount, err = generator.Run(totalTestContext, generateEnrichedBigqueryMessagesFor)
	require.NoError(t, err)
	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	// 7. Verify results in BigQuery
	verificationStart := time.Now()
	logger.Info().Msg("Starting BigQuery verification...")

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
			if !clientIDFound {
				return fmt.Errorf("device ID %s not found in expected map during BQ verification", row.DeviceID)
			}
			require.NotEmpty(t, row.ClientID, "ClientID missing for device %s", row.DeviceID)
			require.NotEmpty(t, row.LocationID, "LocationID missing for device %s", row.DeviceID)
			require.NotEmpty(t, row.Category, "Category missing for device %s", row.DeviceID)
		}
		require.Equal(t, publishedCount, rowCount, "the final number of rows in BigQuery should match the number of messages published")
		return nil
	}

	verifyBigQueryRows(t, logger, totalTestContext, projectID, uniqueDatasetID, uniqueTableID, publishedCount, enrichedBigQueryValidator)

	timings["VerificationDuration"] = time.Since(verificationStart).String()
	timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
}
