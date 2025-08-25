package enrich

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/rs/zerolog"
)

// EnrichmentServiceWrapper wraps the non-generic enrichment pipeline.
type EnrichmentServiceWrapper[K comparable, V any] struct {
	*microservice.BaseServer
	enrichmentService *enrichment.EnrichmentService
	fetcher           cache.Fetcher[K, V]
	logger            zerolog.Logger
	pubsubClient      *pubsub.Client
}

// NewEnrichmentServiceWrapper creates the service with an injected Fetcher and a pipeline-aware enricher.
func NewEnrichmentServiceWrapper[K comparable, V any](
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	fetcher cache.Fetcher[K, V],
	keyExtractor enrichment.KeyExtractor[K],
	applier enrichment.Applier[V],
) (wrapper *EnrichmentServiceWrapper[K, V], err error) {
	enrichmentLogger := logger.With().Str("component", "EnrichmentServiceApp").Logger()

	mainEnricher, err := enrichment.NewEnricherFunc(fetcher.Fetch, keyExtractor, applier, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create enricher function: %w", err)
	}

	compositeEnricher := func(ctx context.Context, msg *messagepipeline.Message) (bool, error) {
		var upstreamMessageData messagepipeline.MessageData
		if err := json.Unmarshal(msg.Payload, &upstreamMessageData); err == nil {
			msg.MessageData = upstreamMessageData
			enrichmentLogger.Debug().Str("msg_id", msg.ID).Msg("Unwrapped upstream message data.")
		}
		return mainEnricher(ctx, msg)
	}

	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID, cfg.ClientConnections["pubsub"]...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults(cfg.InputSubscriptionID)
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, psClient, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults(cfg.OutputTopicID)
	mainProducer, err := messagepipeline.NewGooglePubsubProducer(producerCfg, psClient, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	processor := func(ctx context.Context, msg *messagepipeline.Message) error {
		_, err := mainProducer.Publish(ctx, msg.MessageData)
		return err
	}

	enrichmentService, err := enrichment.NewEnrichmentService(
		enrichment.EnrichmentServiceConfig{NumWorkers: cfg.NumWorkers},
		compositeEnricher,
		consumer,
		processor,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create enrichment service: %w", err)
	}

	baseServer := microservice.NewBaseServer(enrichmentLogger, cfg.HTTPPort)
	return &EnrichmentServiceWrapper[K, V]{
		BaseServer:        baseServer,
		enrichmentService: enrichmentService,
		fetcher:           fetcher,
		logger:            enrichmentLogger,
		pubsubClient:      psClient,
	}, nil
}

// Start initiates the processing service and the embedded HTTP server.
func (s *EnrichmentServiceWrapper[K, V]) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting enrichment server components...")
	if err := s.enrichmentService.Start(ctx); err != nil {
		return fmt.Errorf("failed to start enrichment service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the processing service, closes the fetcher, and the HTTP server.
func (s *EnrichmentServiceWrapper[K, V]) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down enrichment server components...")
	if err := s.enrichmentService.Stop(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Error stopping the enrichment service")
	} else {
		s.logger.Info().Msg("Data processing service stopped.")
	}

	if s.fetcher != nil {
		if err := s.fetcher.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error during fetcher cleanup")
		}
	}

	// Close the pubsub client to release gRPC connections.
	if s.pubsubClient != nil {
		if err := s.pubsubClient.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to close pubsub client.")
		}
	}

	return s.BaseServer.Shutdown(ctx)
}
