package bigqueries

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/illmade-knight/go-dataflow/pkg/bqstore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// BQServiceWrapper wraps the BatchingService for the BigQuery application.
type BQServiceWrapper[T any] struct {
	*microservice.BaseServer
	batchingService *messagepipeline.BatchingService[T]
	bqClient        *bigquery.Client
	pubsubClient    *pubsub.Client
	logger          zerolog.Logger
}

// NewBQServiceWrapper creates and configures a new generic BQServiceWrapper instance.
func NewBQServiceWrapper[T any](
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	transformer messagepipeline.MessageTransformer[T],
) (wrapper *BQServiceWrapper[T], err error) {
	bqLogger := logger.With().Str("component", "BQService").Logger()

	var bqClient *bigquery.Client
	var psClient *pubsub.Client

	defer func() {
		if err != nil {
			if bqClient != nil {
				_ = bqClient.Close()
			}
			if psClient != nil {
				_ = psClient.Close()
			}
		}
	}()

	// 1. Create dependencies (clients).
	var bqOpts []option.ClientOption
	if cfg.ClientConnections != nil {
		bqOpts = cfg.ClientConnections["bigquery"]
	}
	bqClient, err = bigquery.NewClient(ctx, cfg.ProjectID, bqOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	var psOpts []option.ClientOption
	if cfg.ClientConnections != nil {
		psOpts = cfg.ClientConnections["pubsub"]
	}
	psClient, err = pubsub.NewClient(ctx, cfg.ProjectID, psOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	// 2. Create the pipeline components.
	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults()
	consumerCfg.SubscriptionID = cfg.InputSubscriptionID
	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, psClient, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub consumer: %w", err)
	}

	bqInserter, err := bqstore.NewBigQueryInserter[T](ctx, bqClient, &cfg.BigQueryConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery inserter: %w", err)
	}

	// 3. Assemble the pipeline by calling the bqstore service constructor.
	batchingService, err := bqstore.NewBigQueryService[T](
		cfg.BatchProcessing,
		consumer,
		bqInserter,
		transformer,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create bqstore processing service: %w", err)
	}

	baseServer := microservice.NewBaseServer(logger, cfg.HTTPPort)
	return &BQServiceWrapper[T]{
		BaseServer:      baseServer,
		batchingService: batchingService,
		bqClient:        bqClient,
		pubsubClient:    psClient,
		logger:          bqLogger,
	}, nil
}

// Start initiates the BQ processing service and the embedded HTTP server.
func (s *BQServiceWrapper[T]) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting generic BQ server components...")
	if err := s.batchingService.Start(ctx); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the BQ processing service and its components.
func (s *BQServiceWrapper[T]) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down generic BQ server components...")
	if err := s.batchingService.Stop(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Error stopping batching service")
	} else {
		s.logger.Info().Msg("Data processing service stopped.")
	}

	if s.bqClient != nil {
		if err := s.bqClient.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing BigQuery client.")
		}
	}
	if s.pubsubClient != nil {
		if err := s.pubsubClient.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing Pub/Sub client.")
		}
	}
	return s.BaseServer.Shutdown(ctx)
}
