package icestore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/illmade-knight/go-dataflow/pkg/icestore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/rs/zerolog"
)

// IceStoreServiceWrapper wraps the IceStorageService for a common interface.
type IceStoreServiceWrapper struct {
	*microservice.BaseServer
	processingService *icestore.IceStorageService // Corrected type
	pubsubClient      *pubsub.Client
	gcsClient         *storage.Client
	logger            zerolog.Logger
	bucketName        string
	projectID         string
}

// NewIceStoreServiceWrapper creates and configures a new IceStoreServiceWrapper instance.
func NewIceStoreServiceWrapper(
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
) (wrapper *IceStoreServiceWrapper, err error) {
	isLogger := logger.With().Str("component", "IceStoreApp").Logger()

	var gcsClient *storage.Client
	var psClient *pubsub.Client

	defer func() {
		if err != nil {
			isLogger.Error().Err(err).Msg("Failed to initialize IceStoreServiceWrapper, cleaning up resources.")
			if gcsClient != nil {
				_ = gcsClient.Close()
			}
			if psClient != nil {
				_ = psClient.Close()
			}
		}
	}()

	gcsClient, err = storage.NewClient(ctx, cfg.GCSOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	psClient, err = pubsub.NewClient(ctx, cfg.ProjectID, cfg.PubsubOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults()
	consumerCfg.SubscriptionID = cfg.InputSubscriptionID
	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, psClient, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub consumer: %w", err)
	}

	uploaderCfg := icestore.GCSBatchUploaderConfig{
		BucketName:   cfg.IceStore.BucketName,
		ObjectPrefix: cfg.IceStore.ObjectPrefix,
	}

	processingService, err := icestore.NewIceStorageService(
		cfg.ServiceConfig, // Use the corrected config field
		consumer,
		icestore.NewGCSClientAdapter(gcsClient),
		uploaderCfg,
		icestore.ArchivalTransformer,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create processing service: %w", err)
	}

	baseServer := microservice.NewBaseServer(logger, cfg.HTTPPort)
	return &IceStoreServiceWrapper{
		BaseServer:        baseServer,
		processingService: processingService,
		pubsubClient:      psClient,
		gcsClient:         gcsClient,
		logger:            isLogger,
		bucketName:        cfg.IceStore.BucketName,
		projectID:         cfg.ProjectID,
	}, nil
}

// Start initiates the IceStore processing service and the embedded HTTP server.
func (s *IceStoreServiceWrapper) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting IceStore server components...")

	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	s.logger.Info().Str("bucket", s.bucketName).Msg("Ensuring GCS bucket exists.")
	if err := s.gcsClient.Bucket(s.bucketName).Create(createCtx, s.projectID, nil); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			s.logger.Warn().Err(err).Str("bucket", s.bucketName).Msg("Could not create/verify GCS bucket.")
		}
	}

	if err := s.processingService.Start(ctx); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the IceStore processing service and its components.
func (s *IceStoreServiceWrapper) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down IceStore server components...")
	if err := s.processingService.Stop(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Error stopping the processing service")
	} else {
		s.logger.Info().Msg("Data processing service stopped.")
	}

	if s.pubsubClient != nil {
		_ = s.pubsubClient.Close()
	}
	if s.gcsClient != nil {
		_ = s.gcsClient.Close()
	}

	return s.BaseServer.Shutdown(ctx)
}
