//go:build integration

package icestore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/illmade-knight/go-dataflow-services/pkg/icestore"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

// TestPayload defines the structure of a message payload for this test.
type TestPayload struct {
	DeviceID string `json:"device_id"`
	Value    int    `json:"value"`
}

func TestIceStoreServiceWrapper_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)
	const (
		projectID    = "icestore-test-project"
		inputTopicID = "icestore-input-topic"
		inputSubID   = "icestore-input-sub"
		bucketName   = "icestore-test-bucket"
		objectPrefix = "ci-archives/"
		testDeviceID = "test-device-001"
	)

	// --- 1. Setup Emulators ---
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID, map[string]string{inputTopicID: inputSubID}))
	gcsConnection := emulators.SetupGCSEmulator(t, ctx, emulators.GetDefaultGCSConfig(projectID, bucketName))

	// --- 2. Create Test Configuration ---
	cfg := icestore.LoadConfigDefaults(projectID)

	cfg.HTTPPort = ":0"
	cfg.InputSubscriptionID = inputSubID
	cfg.IceStore.BucketName = bucketName
	cfg.IceStore.ObjectPrefix = objectPrefix
	cfg.PubsubOptions = pubsubConnection.ClientOptions
	cfg.GCSOptions = gcsConnection.ClientOptions
	// Use the corrected config field for our test values
	cfg.ServiceConfig.BatchSize = 5
	cfg.ServiceConfig.NumWorkers = 2
	cfg.ServiceConfig.FlushInterval = time.Second

	// --- 3. Create and Start the Service ---
	wrapper, err := icestore.NewIceStoreServiceWrapper(ctx, cfg, logger)
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(ctx)
	t.Cleanup(serviceCancel)
	go func() {
		if startErr := wrapper.Start(serviceCtx); startErr != nil && !errors.Is(startErr, context.Canceled) {
			t.Logf("Service Start() returned an unexpected error: %v", startErr)
		}
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = wrapper.Shutdown(shutdownCtx)
	})

	// --- 4. Publish Test Messages ---
	psClient, err := pubsub.NewClient(ctx, projectID, pubsubConnection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })
	inputTopic := psClient.Topic(inputTopicID)

	const messageCount = 7
	for i := 0; i < messageCount; i++ {
		payloadBytes, err := json.Marshal(&TestPayload{DeviceID: testDeviceID, Value: i})
		require.NoError(t, err)
		res := inputTopic.Publish(ctx, &pubsub.Message{Data: payloadBytes})
		_, err = res.Get(ctx)
		require.NoError(t, err)
	}

	// --- 5. Verification ---
	gcsClient, err := storage.NewClient(ctx, gcsConnection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = gcsClient.Close() })

	require.Eventually(t, func() bool {
		// All 7 messages share the same key (date), so they should be grouped into
		// one batch of 5 and one partial batch of 2. Expect 2 files.
		expectedObjects := 2
		objects, err := listGCSObjectAttrs(ctx, gcsClient.Bucket(bucketName), objectPrefix)
		if err != nil {
			t.Logf("Failed to list GCS objects, retrying: %v", err)
			return false
		}
		return len(objects) == expectedObjects
	}, 15*time.Second, 500*time.Millisecond, "Expected to find 2 objects in GCS, but didn't.")
}

// listGCSObjectAttrs is a test helper to list objects in a GCS bucket.
func listGCSObjectAttrs(ctx context.Context, bucket *storage.BucketHandle, prefix string) ([]*storage.ObjectAttrs, error) {
	var attrs []*storage.ObjectAttrs
	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing objects: %w", err)
		}
		attrs = append(attrs, objAttrs)
	}
	return attrs, nil
}
