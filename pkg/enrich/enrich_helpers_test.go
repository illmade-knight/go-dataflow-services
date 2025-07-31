package enrich_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
)

// BasicKeyExtractor extracts the enrichment key from a message attribute.
func BasicKeyExtractor(msg *messagepipeline.Message) (string, bool) {
	if msg.EnrichmentData != nil {
		if deviceID, ok := msg.EnrichmentData["DeviceID"].(string); ok && deviceID != "" {
			return deviceID, true
		}
	}
	uid, ok := msg.Attributes["uid"]
	return uid, ok
}

// DeviceInfo is the data structure we want to enrich our messages with.
type DeviceInfo struct {
	ClientID   string
	LocationID string
	Category   string
}

// DeviceApplier applies the fetched DeviceInfo to the message's EnrichmentData map.
func DeviceApplier(msg *messagepipeline.Message, data DeviceInfo) {
	if msg.EnrichmentData == nil {
		msg.EnrichmentData = make(map[string]interface{})
	}
	msg.EnrichmentData["name"] = data.ClientID
	msg.EnrichmentData["location"] = data.LocationID
	msg.EnrichmentData["serviceTag"] = data.Category
}

// receiveSingleMessage is a test helper to pull one message from a subscription.
func receiveSingleMessage(t *testing.T, ctx context.Context, sub *pubsub.Subscription, timeout time.Duration) *pubsub.Message {
	t.Helper()
	var receivedMsg *pubsub.Message
	var mu sync.RWMutex

	receiveCtx, receiveCancel := context.WithTimeout(ctx, timeout)
	defer receiveCancel()

	err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		if receivedMsg == nil {
			receivedMsg = msg
			msg.Ack()
			receiveCancel()
		} else {
			msg.Nack()
		}
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("Receive loop ended with error: %v", err)
	}

	mu.RLock()
	defer mu.RUnlock()
	return receivedMsg
}
