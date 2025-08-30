//go:build integration

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/illmade-knight/go-cloud-manager/microservice/servicedirector"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"

	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"

	"github.com/illmade-knight/go-dataflow-services/pkg/bigqueries"
	"github.com/illmade-knight/go-dataflow-services/pkg/enrich"
	"github.com/illmade-knight/go-dataflow-services/pkg/icestore"
	"github.com/illmade-knight/go-dataflow-services/pkg/ingestion"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// startServiceDirector correctly initializes and starts the ServiceDirector for testing.
func startServiceDirector(t *testing.T, ctx context.Context, logger zerolog.Logger, arch *servicemanager.MicroserviceArchitecture) (*servicedirector.Director, string) {
	t.Helper()
	directorCfg := &servicedirector.Config{BaseConfig: microservice.BaseConfig{HTTPPort: ":0"}} // Use a random available port
	director, err := servicedirector.NewServiceDirector(ctx, directorCfg, arch, logger)
	require.NoError(t, err)

	// Start the director's blocking Start() method in a background goroutine.
	go func() {
		if startErr := director.Start(); startErr != nil && !errors.Is(startErr, http.ErrServerClosed) {
			t.Errorf("ServiceDirector failed during test execution: %v", startErr)
		}
	}()

	// Poll the health check endpoint until the service is ready.
	var baseURL string
	require.Eventually(t, func() bool {
		port := director.GetHTTPPort()
		if port == "" || port == ":0" {
			return false // Port not yet assigned
		}
		baseURL = "http://127.0.0.1" + port
		resp, httpErr := http.Get(baseURL + "/healthz")
		if httpErr != nil {
			return false // Server not yet listening
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 15*time.Second, 250*time.Millisecond, "ServiceDirector health check did not become OK")

	return director, baseURL
}

// startIngestionService starts the ingestion service.
func startIngestionService(t *testing.T, ctx context.Context, logger zerolog.Logger, cfg *ingestion.Config) microservice.Service {
	t.Helper()

	cfg.HTTPPort = ":"
	wrapper, err := ingestion.NewIngestionServiceWrapper(ctx, cfg, logger, ingestionEnricher)
	require.NoError(t, err)

	go func() {
		t.Log("starting IngestionService")
		if startErr := wrapper.Start(ctx); startErr != nil && !errors.Is(startErr, http.ErrServerClosed) {
			t.Errorf("IngestionService failed during test execution: %v", startErr)
		}
	}()

	require.Eventually(t, func() bool {
		port := wrapper.GetHTTPPort()
		if port == "" || port == ":0" {
			return false
		}
		resp, httpErr := http.Get(fmt.Sprintf("http://localhost%s/healthz", port))
		if httpErr != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 15*time.Second, 500*time.Millisecond, "IngestionService health check did not become OK")

	return wrapper
}

// startEnrichmentService starts the enrichment service, correctly assembling the cache fetcher.
func startEnrichmentService(t *testing.T, ctx context.Context, logger zerolog.Logger, cfg *enrich.Config, fsClient *firestore.Client) microservice.Service {
	t.Helper()

	// Assemble the Fetcher using the new Decorator Pattern.
	// 1. Create the source of truth (Firestore).
	firestoreFetcher, err := cache.NewFirestore[string, DeviceInfo](ctx, cfg.CacheConfig.FirestoreConfig, fsClient, logger)
	require.NoError(t, err)

	// 2. Create the cache, decorating the Firestore fetcher.
	redisFetcher, err := cache.NewRedisCache[string, DeviceInfo](ctx, &cfg.CacheConfig.RedisConfig, logger, firestoreFetcher)
	require.NoError(t, err)

	cfg.HTTPPort = ":"
	// The wrapper now takes the fully composed fetcher.
	wrapper, err := enrich.NewEnrichmentServiceWrapper[string, DeviceInfo](ctx, cfg, logger, redisFetcher, BasicKeyExtractor, DeviceApplier)
	require.NoError(t, err)

	go func() {
		if startErr := wrapper.Start(ctx); startErr != nil && !errors.Is(startErr, http.ErrServerClosed) {
			t.Errorf("EnrichmentService failed: %v", startErr)
		}
	}()
	return wrapper
}

// startBigQueryService starts the BigQuery service for raw payloads.
func startBigQueryService(t *testing.T, ctx context.Context, logger zerolog.Logger, projectID, subID, datasetID, tableID string, transformer messagepipeline.MessageTransformer[TestPayload]) microservice.Service {
	t.Helper()
	cfg := bigqueries.LoadConfigDefaults(projectID)

	cfg.InputSubscriptionID = subID
	cfg.BigQueryConfig.DatasetID = datasetID
	cfg.BigQueryConfig.TableID = tableID
	// Use the corrected config field
	cfg.BatchProcessing.BatchSize = 15
	cfg.BatchProcessing.FlushInterval = 5 * time.Second

	cfg.HTTPPort = ":"
	wrapper, err := bigqueries.NewBQServiceWrapper[TestPayload](ctx, cfg, logger, transformer)
	require.NoError(t, err)

	go func() {
		if startErr := wrapper.Start(ctx); startErr != nil && !errors.Is(startErr, http.ErrServerClosed) {
			t.Errorf("BigQueryService failed: %v", startErr)
		}
	}()
	return wrapper
}

// startEnrichedBigQueryService starts the BQ service configured to process enriched messages.
func startEnrichedBigQueryService(t *testing.T, ctx context.Context, logger zerolog.Logger, directorURL, projectID, subID, datasetID, tableID, dataflowName string, transformer messagepipeline.MessageTransformer[EnrichedTestPayload]) (microservice.Service, error) {
	t.Helper()
	cfg := bigqueries.LoadConfigDefaults(projectID)

	cfg.ServiceName = "bigquery-enriched-service-e2e"
	cfg.DataflowName = dataflowName
	cfg.ServiceDirectorURL = directorURL
	cfg.InputSubscriptionID = subID
	cfg.BigQueryConfig.DatasetID = datasetID
	cfg.BigQueryConfig.TableID = tableID
	// Use the corrected config field
	cfg.BatchProcessing.BatchSize = 10
	cfg.BatchProcessing.NumWorkers = 2
	cfg.BatchProcessing.FlushInterval = 5 * time.Second

	cfg.HTTPPort = ":"
	wrapper, err := bigqueries.NewBQServiceWrapper[EnrichedTestPayload](ctx, cfg, logger, transformer)
	if err != nil {
		return nil, err
	}

	go func() {
		if startErr := wrapper.Start(ctx); startErr != nil && !errors.Is(startErr, http.ErrServerClosed) {
			t.Errorf("Enriched BigQueryService failed: %v", startErr)
		}
	}()
	return wrapper, nil
}

// startIceStoreService starts the refactored IceStore service.
func startIceStoreService(t *testing.T, ctx context.Context, logger zerolog.Logger, projectID, subID, bucketName, dataflowName string) microservice.Service {
	t.Helper()
	cfg := icestore.LoadConfigDefaults(projectID)

	cfg.DataflowName = dataflowName
	cfg.InputSubscriptionID = subID
	cfg.IceStore.BucketName = bucketName
	// Use the corrected config field
	cfg.ServiceConfig.BatchSize = 15
	cfg.ServiceConfig.FlushInterval = 5 * time.Second

	wrapper, err := icestore.NewIceStoreServiceWrapper(ctx, cfg, logger)
	require.NoError(t, err)

	go func() {
		if startErr := wrapper.Start(ctx); startErr != nil && !errors.Is(startErr, http.ErrServerClosed) {
			t.Errorf("IceStoreService failed: %v", startErr)
		}
	}()
	return wrapper
}
