# **Go Dataflow Microservices**

This repository contains a collection of pre-built, production-ready microservices that form a complete and scalable data processing platform. Each service is a concrete implementation of the patterns and components provided by the go-dataflow library.

Together, these services create a robust pipeline for ingesting, enriching, and storing data from various sources into Google Cloud Platform.

## **Architecture Overview**

The services are designed to be chained together using Google Cloud Pub/Sub as a message bus, creating a decoupled and highly scalable architecture. Each service performs a distinct step in the data's lifecycle.

A typical data flow through this architecture is as follows:

1. **Ingestion Service**: Connects to an external data source (like an MQTT broker), consumes raw data, performs initial lightweight transformation (e.g., extracting a device ID from the topic), and publishes a standardized message to a "raw data" Pub/Sub topic.
2. **Enrichment Service**: Subscribes to the "raw data" topic. For each message, it uses the device ID to fetch detailed metadata from a high-performance, multi-layered cache (L1 In-Memory LRU \-\> L2 Redis \-\> L3 Firestore). It adds this metadata to the message and publishes the newly enriched message to an "enriched data" topic.
3. **Sinking & Archival Services**: Two services subscribe to the "enriched data" topic to handle final storage:
    * **BigQuery Service**: Transforms the enriched data into a structured format and streams it into a BigQuery table for real-time analytics and querying.
    * **IceStore Service**: Archives the complete, enriched message payload to Google Cloud Storage (GCS) for long-term, cost-effective storage. It intelligently groups related messages into single, compressed files to optimize storage and future analysis.

This decoupled flow allows each stage of the pipeline to be scaled, monitored, and updated independently.

## **The Services**

This repository contains four main service packages:

### **1\. ingestion**

The entry point for data. This service is responsible for:

* Connecting to an **MQTT broker**.
* Performing initial, in-flight enrichment (e.g., parsing the MQTT topic).
* Publishing standardized messages to a Pub/Sub topic.
* Providing a /readyz health check based on the MQTT connection status.

### **2\. enrich**

The contextualization engine. This service is responsible for:

* Consuming messages from Pub/Sub.
* Fetching additional data from a multi-layered cache (cache.Fetcher).
* Applying the fetched data to the message.
* Publishing the enriched message to a downstream Pub/Sub topic.
* It is generic and must be initialized with a KeyExtractor and Applier function specific to the data being processed.

### **3\. bigqueries**

The real-time analytics sink. This service is responsible for:

* Consuming messages from Pub/Sub.
* Transforming messages into a specific Go struct that maps to a BigQuery schema.
* Using an efficient, batch-based process to stream data into a **BigQuery table**.
* It is generic and must be initialized with a MessageTransformer for the target BigQuery table schema.

### **4\. icestore**

The long-term archival sink. This service is responsible for:

* Consuming messages from Pub/Sub.
* Grouping messages in memory based on a logical key (e.g., date \+ location).
* Uploading these groups as single, compressed JSONL files to **Google Cloud Storage (GCS)**. This storage layout is highly optimized for cost and for use with external query engines like BigQuery.

## **Getting Started**

Each service package is self-contained and can be run as a standalone application. To use them:

1. Navigate to the directory of the service you want to run (e.g., cmd/ingestion).
2. Review the configuration file (e.g., pkg/ingestion/inconfig.go) to understand the required environment variables and settings.
3. In your main.go, initialize the service's Wrapper struct, providing any necessary custom logic (like transformers or enricher functions).
4. Call the Start() method on the wrapper to begin processing and Shutdown() to perform a graceful stop.

All services are designed to be containerized and deployed in an orchestrated environment like Kubernetes, with support for health checks and graceful shutdown.