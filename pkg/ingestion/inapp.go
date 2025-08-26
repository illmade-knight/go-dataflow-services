package ingestion

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/pubsub/v2"
	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
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
	pubsubClient      *pubsub.Client
}

// NewIngestionServiceWrapper assembles the full ingestion pipeline.
func NewIngestionServiceWrapper(
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	enricher enrichment.MessageEnricher,
) (*IngestionServiceWrapper, error) {

	serviceLogger := logger.With().Str("service", "IngestionService").Logger()

	var psClient *pubsub.Client
	var err error
	psClient, err = pubsub.NewClient(ctx, cfg.ProjectID, cfg.PubsubOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	var consumer *mqttconverter.MqttConsumer
	consumer, err = mqttconverter.NewMqttConsumer(&cfg.MQTT, serviceLogger, cfg.BufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create MQTT consumer: %w", err)
	}

	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults(cfg.OutputTopicID)
	var producer *messagepipeline.GooglePubsubProducer
	producer, err = messagepipeline.NewGooglePubsubProducer(producerCfg, psClient, serviceLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Google Pub/Sub producer: %w", err)
	}

	// Define the final processing step: publish the (now enriched) message.
	processor := func(ctx context.Context, msg *messagepipeline.Message) error {
		_, err := producer.Publish(ctx, msg.MessageData)
		return err
	}

	// Assemble the new, non-generic EnrichmentService.
	var enrichmentService *enrichment.EnrichmentService
	enrichmentService, err = enrichment.NewEnrichmentService(
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
		pubsubClient:      psClient,
	}

	serviceWrapper.registerHandlers()
	return serviceWrapper, nil
}

// Start initiates the background processing services of the ingestion pipeline.
func (s *IngestionServiceWrapper) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting background ingestion components...")
	err := s.enrichmentService.Start(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to start core enrichment service")
		return err
	}
	err = s.BaseServer.Start()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to start HTTP server")
		return err
	}
	s.logger.Info().Msg("Background ingestion components started successfully.")
	return nil
}

// Shutdown gracefully stops the processing service and the HTTP server.
func (s *IngestionServiceWrapper) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down ingestion server components...")
	err := s.enrichmentService.Stop(ctx)
	if err != nil {
		s.logger.Warn().Err(err).Msg("Error during enrichment service shutdown")
	} else {
		s.logger.Info().Msg("Core enrichment service stopped.")
	}

	// Close the pubsub client to release gRPC connections.
	if s.pubsubClient != nil {
		err = s.pubsubClient.Close()
		if err != nil {
			s.logger.Error().Err(err).Msg("Failed to close pubsub client.")
		}
	}

	// REFACTOR: The BaseServer's Shutdown is still called here, which is correct.
	// It ensures that even though the server is started separately in main, it is
	// still shut down as part of the overall service's lifecycle.
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
