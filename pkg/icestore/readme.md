# **IceStore GCS Archival Service**

This package implements a specialized microservice for the long-term archival of data from a Pub/Sub topic to Google Cloud Storage (GCS).

It is built on the go-dataflow/pkg/icestore package and is designed to be a highly efficient "cold storage" pipeline. It intelligently groups messages before upload to optimize storage layout and cost.

## **Core Functionality**

* **Consumes** messages from a specified Pub/Sub subscription.
* **Transforms** each message into a standardized ArchivalData format.
* **Generates a Batch Key**: The transformer creates a logical grouping key for each message, typically based on the date and an attribute like location (e.g., 2025/06/15/location-a).
* **Key-Aware Batching**: The service uses a special KeyAwareBatcher that groups messages in memory based on their generated BatchKey.
* **Grouped Upload**: When a batch for a specific key is full or a timer expires, the entire group of messages is uploaded as a **single, compressed JSONL file** to GCS. The object path is determined by the BatchKey.
* **Manages** the full message lifecycle, including acknowledgments and graceful shutdown.

## **How It Works**

This service is a concrete implementation of the icestore.IceStorageService. Unlike the other, more generic service wrappers, this one is pre-configured to use the icestore.ArchivalTransformer. This transformer is responsible for creating the batchKey that drives the core grouping feature.

The KeyAwareBatcher is the central component. Instead of flushing a batch as soon as it's full, it maintains multiple pending batches in memory—one for each unique key. This ensures that all messages for "location-a" on June 15th end up in the same GCS object, while messages for "location-b" on the same day go into a different object. This co-location of data is extremely valuable for later analysis with tools like BigQuery external tables.

## **How to Use**

This service is largely self-contained. The primary setup involves configuring the service and running it.

### **Example: Initializing the Service**

// in your main.go

func main() {  
ctx := context.Background()  
logger := zerolog.New(os.Stdout)

    // 1\. Load configuration from environment or file  
    cfg := icestore.LoadConfigDefaults("my-gcp-project")  
    cfg.InputSubscriptionID \= "enriched-messages-sub"  
    cfg.IceStore.BucketName \= "my-cold-storage-bucket"  
    cfg.IceStore.ObjectPrefix \= "archives/"  
    cfg.ServiceConfig.BatchSize \= 500 // Tune for your needs  
    cfg.ServiceConfig.FlushInterval \= 5 \* time.Minute

    // 2\. Create the service wrapper  
    wrapper, err := icestore.NewIceStoreServiceWrapper(ctx, cfg, logger)  
    if err \!= nil {  
        logger.Fatal().Err(err).Msg("Failed to create icestore service")  
    }

    // 3\. Start the service and manage its lifecycle  
    // (using a signal handler for graceful shutdown)  
    go func() {  
        if err := wrapper.Start(ctx); err \!= nil {  
            logger.Error().Err(err).Msg("Service failed to start")  
        }  
    }()

    // ... wait for interrupt signal ...

    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30\*time.Second)  
    defer cancel()  
    if err := wrapper.Shutdown(shutdownCtx); err \!= nil {  
        logger.Error().Err(err).Msg("Service shutdown failed")  
    }  
}

## **Configuration**

The service is configured via the Config struct in icconfig.go. Key settings include:

* ProjectID: Your Google Cloud project ID.
* InputSubscriptionID: The Pub/Sub subscription to archive messages from.
* IceStore.BucketName: The target GCS bucket.
* IceStore.ObjectPrefix: A prefix to use for all archived objects (e.g., data/raw/).
* ServiceConfig: Contains NumWorkers, BatchSize, and FlushInterval to control the key-aware batching behavior.