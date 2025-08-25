package enrich_test

import (
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
