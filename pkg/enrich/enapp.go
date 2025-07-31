package enrich

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/rs/zerolog"
)

// EnrichmentServiceWrapper wraps the non-generic enrichment pipeline.
type EnrichmentServiceWrapper[K comparable, V any] struct {
	*microservice.BaseServer
	enrichmentService *enrichment.EnrichmentService
	fetcherCleanup    func() error
	logger            zerolog.Logger
}

// NewEnrichmentServiceWrapper creates the service with injected clients for testability.
func NewEnrichmentServiceWrapper[K comparable, V any](
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	fetcher enrichment.CacheFetcher[K, V],
	keyExtractor enrichment.KeyExtractor[K],
	applier enrichment.Applier[V],
) (wrapper *EnrichmentServiceWrapper[K, V], err error) {
	enrichmentLogger := logger.With().Str("component", "EnrichmentServiceApp").Logger()

	// 1. Create the MessageEnricher function using the enrichment library.
	enricher, err := enrichment.NewEnricherFunc(fetcher.Fetch, keyExtractor, applier, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create enricher function: %w", err)
	}

	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID, cfg.ClientConnections["pubsub"]...)

	// 2. Create the pipeline components (consumer and producer).
	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults()
	consumerCfg.SubscriptionID = cfg.InputSubscriptionID
	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, psClient, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults()
	producerCfg.TopicID = cfg.OutputTopicID
	mainProducer, err := messagepipeline.NewGooglePubsubProducer(ctx, producerCfg, psClient, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	// 3. Define the MessageProcessor function that publishes the enriched message.
	processor := func(ctx context.Context, msg *messagepipeline.Message) error {
		_, err := mainProducer.Publish(ctx, msg.MessageData)
		return err
	}

	// 4. Assemble the final EnrichmentService.
	enrichmentService, err := enrichment.NewEnrichmentService(
		enrichment.EnrichmentServiceConfig{NumWorkers: cfg.NumWorkers},
		enricher,
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
		logger:            enrichmentLogger,
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

// Shutdown gracefully stops the processing service and the HTTP server.
func (s *EnrichmentServiceWrapper[K, V]) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down enrichment server components...")
	if err := s.enrichmentService.Stop(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Error stopping the enrichment service")
	} else {
		s.logger.Info().Msg("Data processing service stopped.")
	}

	if s.fetcherCleanup != nil {
		if err := s.fetcherCleanup(); err != nil {
			s.logger.Error().Err(err).Msg("Error during fetcher cleanup")
		}
	}
	return s.BaseServer.Shutdown(ctx)
}
