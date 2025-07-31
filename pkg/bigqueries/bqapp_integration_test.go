//go:build integration

package bigqueries_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/illmade-knight/go-dataflow-services/pkg/bigqueries"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// TestPayload is the concrete data structure for this integration test.
type TestPayload struct {
	MessageID string    `bigquery:"message_id"`
	Data      string    `bigquery:"data"`
	Timestamp time.Time `bigquery:"timestamp"`
}

func TestBQServiceWrapper_Integration(t *testing.T) {
	testContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)
	const (
		projectID = "bq-service-test-project"
		topicID   = "bq-service-input-topic"
		subID     = "bq-service-input-sub"
		datasetID = "test_dataset"
		tableID   = "test_table"
	)

	// --- 1. Setup Emulators ---
	pubsubConn := emulators.SetupPubsubEmulator(t, testContext, emulators.GetDefaultPubsubConfig(projectID, nil))
	bqSchema := map[string]interface{}{tableID: TestPayload{}}
	bqConn := emulators.SetupBigQueryEmulator(t, testContext, emulators.GetDefaultBigQueryConfig(projectID, map[string]string{datasetID: tableID}, bqSchema))

	// --- 2. Setup Pub/Sub Resources ---
	psClient, err := pubsub.NewClient(testContext, projectID, pubsubConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	inputTopic, err := psClient.CreateTopic(testContext, topicID)
	require.NoError(t, err)
	_, err = psClient.CreateSubscription(testContext, subID, pubsub.SubscriptionConfig{Topic: inputTopic})
	require.NoError(t, err)

	// --- 3. Create Test Configuration ---
	cfg := bigqueries.LoadConfigDefaults(projectID)
	cfg.HTTPPort = ":0"
	cfg.InputSubscriptionID = subID
	cfg.BigQueryConfig.DatasetID = datasetID
	cfg.BigQueryConfig.TableID = tableID
	cfg.ClientConnections = map[string][]option.ClientOption{
		"bigquery": bqConn.ClientOptions,
		"pubsub":   pubsubConn.ClientOptions,
	}
	cfg.BatchProcessing.NumWorkers = 2
	cfg.BatchProcessing.BatchSize = 5
	cfg.BatchProcessing.FlushInterval = 1 * time.Second

	// --- 4. Define the Transformer ---
	transformer := func(ctx context.Context, msg *messagepipeline.Message) (*TestPayload, bool, error) {
		return &TestPayload{
			MessageID: msg.ID,
			Data:      string(msg.Payload),
			Timestamp: msg.PublishTime,
		}, false, nil
	}

	// --- 5. Create and Start the Service ---
	wrapper, err := bigqueries.NewBQServiceWrapper[TestPayload](testContext, cfg, logger, transformer)
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(testContext)
	t.Cleanup(serviceCancel)
	go func() {
		if startErr := wrapper.Start(serviceCtx); startErr != nil {
			t.Logf("Service Start() returned an error: %v", startErr)
		}
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = wrapper.Shutdown(shutdownCtx)
	})

	const messageCount = 7
	for i := 0; i < messageCount; i++ {
		res := inputTopic.Publish(testContext, &pubsub.Message{Data: []byte(fmt.Sprintf("message-%d", i))})
		_, err = res.Get(testContext)
		require.NoError(t, err)
	}

	// --- 6. Verification ---
	bqClient, err := bigquery.NewClient(testContext, projectID, bqConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bqClient.Close() })

	getRowCount := func() (int, error) {
		q := bqClient.Query(fmt.Sprintf("SELECT count(*) FROM `%s.%s`", datasetID, tableID))
		it, err := q.Read(testContext)
		if err != nil {
			return -1, err
		}
		var row []bigquery.Value
		err = it.Next(&row)
		if err != nil && !errors.Is(err, iterator.Done) {
			return -1, err
		}
		if len(row) == 0 {
			return 0, nil
		}
		return int(row[0].(int64)), nil
	}

	require.Eventually(t, func() bool {
		count, err := getRowCount()
		if err != nil {
			return false
		}
		return count == messageCount
	}, 20*time.Second, 500*time.Millisecond, "Expected to find %d rows in BigQuery", messageCount)
}
