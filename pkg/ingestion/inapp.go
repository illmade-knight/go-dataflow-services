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
	// REFACTOR: Store a map of producers, keyed by route name.
	producers map[string]*messagepipeline.GooglePubsubProducer
}

// NewIngestionServiceWrapper assembles the full ingestion pipeline with dynamic routing.
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

	// REFACTOR: Dynamically build TopicMappings from the routing configuration.
	if len(cfg.Routing) == 0 {
		return nil, fmt.Errorf("no routes defined in the configuration")
	}
	topicMappings := make([]mqttconverter.TopicMapping, 0, len(cfg.Routing))
	for routeName, routeCfg := range cfg.Routing {
		topicMappings = append(topicMappings, mqttconverter.TopicMapping{
			Name:  routeName,
			Topic: routeCfg.MqttTopic,
			QoS:   routeCfg.QoS,
		})
	}
	// The core MQTT client config is still used, but we overwrite the TopicMappings.
	mqttCfg := cfg.MQTT
	mqttCfg.TopicMappings = topicMappings

	consumer, err := mqttconverter.NewMqttConsumer(&mqttCfg, serviceLogger, cfg.BufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create MQTT consumer: %w", err)
	}

	// REFACTOR: Create a producer for each configured route destination.
	producers := make(map[string]*messagepipeline.GooglePubsubProducer)
	for routeName, producerCfg := range cfg.Producers {
		// Ensure every producer destination has a corresponding route source.
		if _, ok := cfg.Routing[routeName]; !ok {
			return nil, fmt.Errorf("producer defined for route '%s' but no corresponding routing rule exists", routeName)
		}
		pubsubProducerCfg := messagepipeline.NewGooglePubsubProducerDefaults(producerCfg.OutputTopicID)
		producer, err := messagepipeline.NewGooglePubsubProducer(pubsubProducerCfg, psClient, serviceLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create producer for route '%s' (topic: %s): %w", routeName, producerCfg.OutputTopicID, err)
		}
		producers[routeName] = producer
		serviceLogger.Info().Str("route", routeName).Str("topic", producerCfg.OutputTopicID).Msg("Initialized producer for route")
	}

	// REFACTOR: The processor is now a "smart router".
	processor := func(ctx context.Context, msg *messagepipeline.Message) error {
		routeName, ok := msg.Attributes["route_name"]
		if !ok {
			return fmt.Errorf("message %s is missing 'route_name' attribute for routing", msg.ID)
		}

		producer, ok := producers[routeName]
		if !ok {
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
		producers:         producers, // Store the map of producers.
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

// Shutdown gracefully stops the processing service and all producers.
func (s *IngestionServiceWrapper) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down ingestion server components...")
	err := s.enrichmentService.Stop(ctx)
	if err != nil {
		s.logger.Warn().Err(err).Msg("Error during enrichment service shutdown")
	} else {
		s.logger.Info().Msg("Core enrichment service stopped.")
	}

	// REFACTOR: Stop all configured producers.
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
