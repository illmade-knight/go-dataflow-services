//go:build integration

// Package e2e contains end-to-end tests for dataflow pipelines.
// This file provides generic, reusable helper functions for verifying test outcomes
// in various Google Cloud services.
package e2e

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

// MessageValidationFunc defines a function for custom validation logic on a Pub/Sub message.
// It is injected into the generic verifier to check message contents.
// It should return 'true' if the message is valid and should be counted.
type MessageValidationFunc func(t *testing.T, msg *pubsub.Message) bool

// verifyPubSubMessages is a generic, reusable verifier for Pub/Sub topics.
// It listens on a subscription until an expected number of messages (sent via a channel)
// has been received. It can use an optional validation function to filter which
// messages count toward the total.
func verifyPubSubMessages(
	t *testing.T,
	logger zerolog.Logger,
	ctx context.Context,
	sub *pubsub.Subscriber,
	expectedCountCh <-chan int,
	validator MessageValidationFunc,
) int {
	t.Helper()

	verifierLogger := logger.With().Str("component", "PubSubVerifier").Str("subscription", sub.ID()).Logger()

	var receivedCount atomic.Int32
	var targetCount atomic.Int32
	targetCount.Store(-1) // Initialize to -1 to indicate no target set yet

	var mu sync.Mutex
	milestoneTimings := make(map[string]time.Duration)
	receiverStartTime := time.Now()

	receiveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Goroutine to listen for the expected count from the load generator
	go func() {
		verifierLogger.Info().Msg("Verifier is ready and listening for messages and expected count...")

		select {
		case count, ok := <-expectedCountCh:
			if !ok {
				verifierLogger.Warn().Msg("Expected count channel was closed before a count was sent.")
				return
			}
			targetCount.Store(int32(count))
			verifierLogger.Info().Int("expected_exact", count).Msg("Verifier received exact count from load generator.")

			// Check if target is already met by messages that arrived very quickly
			if receivedCount.Load() >= targetCount.Load() {
				verifierLogger.Info().Msg("Target count already met on receipt of target count.")
				cancel() // Cancel receiveCtx if target already met
			}
		case <-receiveCtx.Done():
			verifierLogger.Info().Msg("Receive context cancelled. Stopping expected count listener.")
			return
		}
	}()

	// Start receiving messages
	err := sub.Receive(receiveCtx, func(receiveCallbackCtx context.Context, msg *pubsub.Message) {
		msg.Ack()

		if validator != nil {
			if !validator(t, msg) {
				return
			}
		}

		newCount := receivedCount.Add(1)

		mu.Lock()
		if newCount == 1 {
			milestoneTimings["First Valid Message"] = time.Since(receiverStartTime)
		}

		if targetCount.Load() >= 0 && newCount >= targetCount.Load() {
			if _, ok := milestoneTimings["Final Valid Message"]; !ok {
				milestoneTimings["Final Valid Message"] = time.Since(receiverStartTime)
			}
			cancel()
		}
		mu.Unlock()
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Pub/Sub Receive returned an unexpected error: %v", err)
	}

	finalCount := int(receivedCount.Load())
	verifierLogger.Info().Int("final_valid_count", finalCount).Msg("Verifier finished receiving.")

	t.Log("--- Verification Milestone Timings ---")
	mu.Lock()
	for name, duration := range milestoneTimings {
		t.Logf("Time to receive %-20s: %v", name, duration)
	}
	mu.Unlock()
	t.Log("------------------------------------")

	if targetCount.Load() >= 0 {
		require.Equal(t, int(targetCount.Load()), finalCount, "Did not receive the exact number of expected (and valid) messages.")
	} else {
		verifierLogger.Warn().Msg("No valid expected count was ever provided. Cannot assert exact message count.")
	}
	return finalCount
}

// BQRowValidator defines a function to validate a set of BigQuery rows.
// It returns an error if validation fails, which the polling function will catch.
type BQRowValidator func(t *testing.T, iter *bq.RowIterator) error

// verifyBigQueryRows polls BigQuery until a target row count is met,
// then runs a final validation using the provided validator function.
func verifyBigQueryRows(
	t *testing.T,
	logger zerolog.Logger,
	ctx context.Context,
	projectID, datasetID, tableID string,
	expectedCount int,
	validator BQRowValidator,
) {
	t.Helper()
	verifierLogger := logger.With().Str("component", "BQVerifier").Str("table", tableID).Logger()

	client, err := bq.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	// 1. Poll until the expected number of rows appears.
	countQuery := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s.%s`", projectID, datasetID, tableID)
	require.Eventually(t, func() bool {
		q := client.Query(countQuery)
		it, err := q.Read(ctx)
		if err != nil {
			verifierLogger.Warn().Err(err).Msg("Polling: Failed to query BQ for row count")
			return false
		}
		var row struct{ Count int64 }
		if err := it.Next(&row); err != nil {
			return false
		}
		verifierLogger.Info().Int64("current_count", row.Count).Int("expected", expectedCount).Msg("Polling BQ results...")
		return row.Count >= int64(expectedCount)
	}, 90*time.Second, 5*time.Second, "BigQuery row count did not meet threshold in time")

	// 2. Once the count is met, run the full validation logic.
	verifierLogger.Info().Msg("Expected row count reached. Running final validation...")
	fullQuery := fmt.Sprintf("SELECT * FROM `%s.%s.%s`", projectID, datasetID, tableID)
	it, err := client.Query(fullQuery).Read(ctx)
	require.NoError(t, err, "Failed to query for full BQ results")

	err = validator(t, it)
	require.NoError(t, err, "BigQuery row validation failed")

	verifierLogger.Info().Msg("BigQuery validation successful!")
}

// verifyGCSResults polls GCS and counts the total number of records across all
// JSONL files in a bucket until an exact expected count is reached.
func verifyGCSResults(t *testing.T, logger zerolog.Logger, ctx context.Context, gcsClient *storage.Client, bucketName string, expectedCount int) {
	t.Helper()
	verifierLogger := logger.With().Str("component", "GCSVerifier").Str("bucket", bucketName).Logger()

	var totalCount int
	require.Eventually(t, func() bool {
		totalCount = 0
		bucket := gcsClient.Bucket(bucketName)
		it := bucket.Objects(ctx, nil)
		for {
			attrs, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				verifierLogger.Warn().Err(err).Msg("Failed to list GCS objects during polling")
				return false
			}

			rc, err := bucket.Object(attrs.Name).NewReader(ctx)
			if err != nil {
				verifierLogger.Warn().Err(err).Str("object", attrs.Name).Msg("Failed to create GCS object reader")
				continue
			}
			// should we use defer in loop here, check this?
			defer func() {
				_ = rc.Close()
			}()

			gzr, err := gzip.NewReader(rc)
			if err != nil {
				verifierLogger.Warn().Err(err).Str("object", attrs.Name).Msg("Failed to create gzip reader")
				continue
			}
			// should we use defer in loop here?
			defer func() {
				_ = gzr.Close()
			}()

			scanner := bufio.NewScanner(gzr)
			for scanner.Scan() {
				totalCount++
			}
		}

		verifierLogger.Info().Int("current_count", totalCount).Int("expected_exact", expectedCount).Msg("Polling GCS results...")
		return totalCount >= expectedCount

	}, 90*time.Second, 5*time.Second, "GCS object count did not meet threshold in time")

	require.Equal(t, expectedCount, totalCount, "Did not find the exact number of expected records in GCS")
}
