//go:build integration

// Package e2e contains end-to-end tests for dataflow pipelines.
// This test file, bigqueryflow_test.go, validates the dataflow from MQTT ingestion
// to a Pub/Sub topic, and then from that topic to a BigQuery sink.
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
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-dataflow-services/pkg/ingestion"
	"github.com/illmade-knight/go-test/auth"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/illmade-knight/go-test/loadgen"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	generateSimpleBigqueryMessagesFor = 5 * time.Second
	fullBigQueryTestNumDevices        = 5
	fullBigQueryTestRate              = 2.0
)

func TestFullDataflowE2E(t *testing.T) {
	projectID := auth.CheckGCPAuth(t)

	timings := make(map[string]string)
	testStart := time.Now()
	var publishedCount int
	var expectedMessageCount int

	totalTestContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

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

	logger := log.With().Str("test", "TestFullDataflowE2E").Logger()

	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("bq-flow-%s", runID)
	uniqueTopicID := fmt.Sprintf("dev-ingestion-topic-%s", runID)
	uniqueSubID := fmt.Sprintf("dev-bq-subscription-%s", runID)
	uniqueDatasetID := fmt.Sprintf("dev_dataflow_dataset_%s", runID)
	uniqueTableID := fmt.Sprintf("dev_ingested_payloads_%s", runID)
	logger.Info().Str("run_id", runID).Msg("Generated unique resources for test run")

	schemaIdentifier := "github.com/illmade-knight/go-dataflow-services/dataflow/devflow/e2e.TestPayload"
	servicemanager.RegisterSchema(schemaIdentifier, TestPayload{})
	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e",
			ProjectID: projectID,
			Location:  "US",
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics:           []servicemanager.TopicConfig{{CloudResource: servicemanager.CloudResource{Name: uniqueTopicID}}},
					Subscriptions:    []servicemanager.SubscriptionConfig{{CloudResource: servicemanager.CloudResource{Name: uniqueSubID}, Topic: uniqueTopicID}},
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

	var opts []option.ClientOption
	if creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}
	bqClient, err := bigquery.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bqClient.Close() })

	start := time.Now()
	mqttContainer := emulators.SetupMosquittoContainer(t, totalTestContext, emulators.GetDefaultMqttImageContainer())
	timings["EmulatorSetup(MQTT)"] = time.Since(start).String()

	start = time.Now()
	directorService, directorURL := startServiceDirector(t, totalTestContext, logger.With().Str("service", "servicedirector").Logger(), servicesConfig)
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
		logger.Info().Msg("Requesting resource teardown from ServiceDirector...")
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()

		err = directorService.TeardownDataflow(cleanupCtx, dataflowName)
		if err != nil {
			logger.Warn().Err(err).Msg("cleanup call failed")
		}
		ds := bqClient.Dataset(uniqueDatasetID)
		if err := ds.DeleteWithContents(cleanupCtx); err != nil {
			logger.Warn().Err(err).Str("dataset", uniqueDatasetID).Msg("Failed to delete BigQuery dataset during cleanup.")
		}
		timings["CloudResourceTeardown"] = time.Since(teardownStart).String()
	})

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
		"uplinks": {OutputTopicID: uniqueTopicID},
	}

	cfg.MQTT.BrokerURL = mqttContainer.EmulatorAddress
	ingestionLogger := logger.With().Str("service", "ingestion").Logger()
	ingestionSvc := startIngestionService(t, totalTestContext, ingestionLogger, cfg)
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = ingestionSvc.Shutdown(shutdownCtx)
	})

	start = time.Now()
	bqLogger := logger.With().Str("service", "bigquery").Logger()
	bqSvc := startBigQueryService(t, totalTestContext, bqLogger, projectID, uniqueSubID, uniqueDatasetID, uniqueTableID, bqTransformer)
	timings["ServiceStartup(BigQuery)"] = time.Since(start).String()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = bqSvc.Shutdown(shutdownCtx)
	})
	logger.Info().Msg("All services started successfully.")

	loadgenStart := time.Now()
	logger.Info().Msg("Starting MQTT load generator...")
	loadgenClient := loadgen.NewMqttClient(mqttContainer.EmulatorAddress, "devices/%s/data", 1, logger)
	devices := make([]*loadgen.Device, fullBigQueryTestNumDevices)
	for i := 0; i < fullBigQueryTestNumDevices; i++ {
		devices[i] = &loadgen.Device{ID: fmt.Sprintf("e2e-bq-device-%d-%s", i, runID), MessageRate: fullBigQueryTestRate, PayloadGenerator: &testPayloadGenerator{}}
	}
	generator := loadgen.NewLoadGenerator(loadgenClient, devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateSimpleBigqueryMessagesFor)

	publishedCount, err = generator.Run(totalTestContext, generateSimpleBigqueryMessagesFor)
	require.NoError(t, err)
	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	verificationStart := time.Now()
	logger.Info().Msg("Starting BigQuery verification...")

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
		require.Equal(t, publishedCount, rowCount, "the final number of rows in BigQuery should match the number of messages published")
		return nil
	}

	verifyBigQueryRows(t, logger, totalTestContext, projectID, uniqueDatasetID, uniqueTableID, publishedCount, countValidator)

	timings["VerificationDuration"] = time.Since(verificationStart).String()
	timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
}
