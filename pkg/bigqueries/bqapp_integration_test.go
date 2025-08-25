//go:build integration

package bigqueries_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow-services/pkg/bigqueries"
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

func TestBQServiceWrapper_Integration(t *testing.T) {
	testContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)
	// REFACTOR: Use unique names for test resources.
	runID := uuid.NewString()
	const projectID = "bq-service-test-project"
	topicID := "bq-service-input-topic-" + runID
	subID := "bq-service-input-sub-" + runID
	const datasetID = "test_dataset"
	const tableID = "test_table"

	// --- 1. Setup Emulators ---
	// REFACTOR: Use the updated GetDefaultPubsubConfig.
	pubsubConn := emulators.SetupPubsubEmulator(t, testContext, emulators.GetDefaultPubsubConfig(projectID))
	bqSchema := map[string]interface{}{tableID: TestPayload{}}
	bqConn := emulators.SetupBigQueryEmulator(t, testContext, emulators.GetDefaultBigQueryConfig(projectID, map[string]string{datasetID: tableID}, bqSchema))

	// --- 2. Setup Pub/Sub Resources ---
	psClient, err := pubsub.NewClient(testContext, projectID, pubsubConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	// REFACTOR: Create test-specific pubsub resources.
	createPubsubResources(t, testContext, psClient, projectID, topicID, subID)

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
		if startErr := wrapper.Start(serviceCtx); startErr != nil && !errors.Is(startErr, context.Canceled) {
			t.Logf("Service Start() returned an error: %v", startErr)
		}
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = wrapper.Shutdown(shutdownCtx)
	})

	// REFACTOR: Use the v2 publisher.
	publisher := psClient.Publisher(topicID)
	defer publisher.Stop()
	const messageCount = 7
	for i := 0; i < messageCount; i++ {
		res := publisher.Publish(testContext, &pubsub.Message{Data: []byte(fmt.Sprintf("message-%d", i))})
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
