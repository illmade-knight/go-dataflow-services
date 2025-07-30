package ingestion

import (
	"context"
	"fmt"
	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"net/http"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
)

// IngestionServiceWrapper now wraps the non-generic EnrichmentService.
type IngestionServiceWrapper struct {
	*microservice.BaseServer
	consumer          *mqttconverter.MqttConsumer
	enrichmentService *enrichment.EnrichmentService
	logger            zerolog.Logger
}

// NewIngestionServiceWrapper assembles the full ingestion pipeline.
func NewIngestionServiceWrapper(
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	enricher enrichment.MessageEnricher,
) (*IngestionServiceWrapper, error) {

	serviceLogger := logger.With().Str("service", "IngestionService").Logger()

	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID, cfg.PubsubOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	consumer, err := mqttconverter.NewMqttConsumer(&cfg.MQTT, serviceLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create MQTT consumer: %w", err)
	}

	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults()
	producer, err := messagepipeline.NewGooglePubsubProducer(ctx, producerCfg, psClient, serviceLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Google Pub/Sub producer: %w", err)
	}

	// Define the final processing step: publish the (now enriched) message.
	processor := func(ctx context.Context, msg *messagepipeline.Message) error {
		_, err := producer.Publish(ctx, msg.MessageData)
		return err
	}

	// Assemble the new, non-generic EnrichmentService.
	enrichmentService, err := enrichment.NewEnrichmentService(
		enrichment.EnrichmentServiceConfig{NumWorkers: cfg.NumWorkers},
		enricher,
		consumer,
		processor,
		serviceLogger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create enrichment service: %w", err)
	}

	baseServer := microservice.NewBaseServer(logger, cfg.HTTPPort)

	serviceWrapper := &IngestionServiceWrapper{
		BaseServer:        baseServer,
		consumer:          consumer,
		enrichmentService: enrichmentService,
		logger:            serviceLogger,
	}

	serviceWrapper.registerHandlers()
	return serviceWrapper, nil
}

// Start initiates the processing service and the embedded HTTP server.
func (s *IngestionServiceWrapper) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting ingestion service components...")
	if err := s.enrichmentService.Start(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Failed to start core enrichment service")
		return err
	}
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the processing service and the HTTP server.
func (s *IngestionServiceWrapper) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down ingestion server components...")
	if err := s.enrichmentService.Stop(ctx); err != nil {
		s.logger.Warn().Err(err).Msg("Error during enrichment service shutdown")
	} else {
		s.logger.Info().Msg("Core enrichment service stopped.")
	}
	return s.BaseServer.Shutdown(ctx)
}

func (s *IngestionServiceWrapper) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}
func (s *IngestionServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
func (s *IngestionServiceWrapper) registerHandlers() {
	mux := s.Mux()
	mux.HandleFunc("/readyz", s.readinessCheck)
}
func (s *IngestionServiceWrapper) readinessCheck(w http.ResponseWriter, _ *http.Request) {
	if s.consumer.IsConnected() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("READY"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("NOT READY"))
}
