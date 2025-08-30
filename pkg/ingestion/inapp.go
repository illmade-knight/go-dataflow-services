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

// IngestionServiceWrapper encapsulates the entire ingestion pipeline, including the MQTT consumer,
// the message enrichment service, and the dynamic routing to multiple Pub/Sub producers.
type IngestionServiceWrapper struct {
	*microservice.BaseServer
	consumer          *mqttconverter.MqttConsumer
	enrichmentService *enrichment.EnrichmentService
	logger            zerolog.Logger
	pubsubClient      *pubsub.Client
	// producers holds a map of Pub/Sub producers, keyed by the logical route name
	// defined in the service configuration (e.g., "uplinks", "joins").
	producers map[string]*messagepipeline.GooglePubsubProducer
}

// NewIngestionServiceWrapper assembles and validates the full ingestion pipeline.
// It performs the following key steps:
//  1. Creates a Pub/Sub client for all producer connections.
//  2. Dynamically builds the list of MQTT topic subscriptions from the routing configuration.
//  3. Initializes a map of Pub/Sub producers, one for each unique destination route.
//  4. Configures a "smart" processor function that inspects incoming messages and uses their
//     'route_name' attribute to select the correct producer for publishing.
//  5. Wires all components (consumer, enricher, processor) into a cohesive enrichment service.
func NewIngestionServiceWrapper(
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	enricher enrichment.MessageEnricher,
) (*IngestionServiceWrapper, error) {

	serviceLogger := logger.With().Str("service", "IngestionService").Logger()

	psClient, err := pubsub.NewClient(ctx, cfg.BaseConfig.ProjectID, cfg.PubsubOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	// Dynamically build the TopicMappings for the MQTT consumer from the routing configuration.
	if len(cfg.Routing) == 0 {
		return nil, fmt.Errorf("no routes defined in the configuration")
	}

	// BUG FIX: The TopicMapping's `Name` must match the producer's key. The producer key
	// is the canonical `destination.route_name` from the YAML, not the top-level
	// descriptive key for the route entry. This change aligns the consumer's tagging
	// with the processor's routing lookup.
	topicMappings := make([]mqttconverter.TopicMapping, 0, len(cfg.Routing))
	for _, routeCfg := range cfg.Routing {
		topicMappings = append(topicMappings, mqttconverter.TopicMapping{
			Name:  routeCfg.DestinationRouteName, // Use the destination route name for tagging.
			Topic: routeCfg.MqttTopic,
			QoS:   routeCfg.QoS,
		})
	}

	// Overwrite the TopicMappings in the MQTT config with our dynamically generated list.
	mqttCfg := cfg.MQTT
	mqttCfg.TopicMappings = topicMappings

	consumer, err := mqttconverter.NewMqttConsumer(&mqttCfg, serviceLogger, cfg.BufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create MQTT consumer: %w", err)
	}

	// Create a producer for each configured route destination.
	producers := make(map[string]*messagepipeline.GooglePubsubProducer)
	for routeName, producerCfg := range cfg.Producers {
		pubsubProducerCfg := messagepipeline.NewGooglePubsubProducerDefaults(producerCfg.OutputTopicID)
		producer, pErr := messagepipeline.NewGooglePubsubProducer(pubsubProducerCfg, psClient, serviceLogger)
		if pErr != nil {
			return nil, fmt.Errorf("failed to create producer for route '%s' (topic: %s): %w", routeName, producerCfg.OutputTopicID, pErr)
		}
		producers[routeName] = producer
		serviceLogger.Info().Str("route", routeName).Str("topic", producerCfg.OutputTopicID).Msg("Initialized producer for route")
	}

	// This processor function acts as a smart router. It inspects the message's
	// 'route_name' attribute (added by the MqttConsumer) and selects the
	// appropriate producer from the map.
	processor := func(ctx context.Context, msg *messagepipeline.Message) error {
		routeName, ok := msg.Attributes["route_name"]
		if !ok {
			return fmt.Errorf("message %s is missing 'route_name' attribute for routing", msg.ID)
		}

		producer, ok := producers[routeName]
		if !ok {
			// This case should be caught by the config validation at startup, but provides a runtime safeguard.
			return fmt.Errorf("no producer configured for route: '%s'", routeName)
		}

		_, err := producer.Publish(ctx, msg.MessageData)
		return err
	}

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
		pubsubClient:      psClient,
		producers:         producers,
	}

	serviceWrapper.registerHandlers()
	return serviceWrapper, nil
}

// Start initiates the background processing services of the ingestion pipeline,
// including the MQTT consumer and the HTTP server for health checks.
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

// Shutdown gracefully stops all components of the service in the reverse order of startup.
// This includes the core processing pipeline, all Pub/Sub producers, the gRPC client,
// and the HTTP server.
func (s *IngestionServiceWrapper) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down ingestion server components...")
	err := s.enrichmentService.Stop(ctx)
	if err != nil {
		s.logger.Warn().Err(err).Msg("Error during enrichment service shutdown")
	} else {
		s.logger.Info().Msg("Core enrichment service stopped.")
	}

	// Stop all configured producers to ensure all buffered messages are flushed.
	for routeName, producer := range s.producers {
		s.logger.Info().Str("route", routeName).Msg("Stopping producer...")
		if pErr := producer.Stop(ctx); pErr != nil {
			s.logger.Warn().Err(pErr).Str("route", routeName).Msg("Error stopping producer")
		}
	}

	if s.pubsubClient != nil {
		err = s.pubsubClient.Close()
		if err != nil {
			s.logger.Error().Err(err).Msg("Failed to close pubsub client.")
		}
	}

	return s.BaseServer.Shutdown(ctx)
}

// Mux returns the underlying HTTP ServeMux for the service.
func (s *IngestionServiceWrapper) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

// GetHTTPPort returns the resolved port the HTTP server is listening on.
func (s *IngestionServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}

// registerHandlers sets up the HTTP endpoints for the service.
func (s *IngestionServiceWrapper) registerHandlers() {
	mux := s.Mux()
	mux.HandleFunc("/readyz", s.readinessCheck)
}

// readinessCheck is an HTTP handler that provides a readiness probe for orchestrators.
// It returns HTTP 200 OK if the MQTT consumer is connected, and 503 Service Unavailable otherwise.
func (s *IngestionServiceWrapper) readinessCheck(w http.ResponseWriter, _ *http.Request) {
	if s.consumer.IsConnected() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("READY"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("NOT READY"))
}
