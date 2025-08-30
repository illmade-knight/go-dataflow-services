# **Go Dataflow Services & E2E Tests**

This repository contains production-grade microservice "wrappers" and a comprehensive end-to-end (E2E) testing suite for the packages found in the [go-dataflow](https://www.google.com/search?q=go-dataflow) core library.

The primary purpose of this repository is to provide concrete, deployable service implementations that can be used as the foundation for a real-world data platform. It demonstrates how to compose the generic building blocks from the core library into fully-fledged, configurable, and highly testable microservices.

## **Microservice Wrappers (/pkg)**

The /pkg directory contains the service wrappers. Each wrapper is a Go package that takes a generic pipeline component from the go-dataflow library (like the icestore or bqstore) and wraps it in a microservice.Service interface.

This provides:

* **A Standardized Interface**: All services have a consistent Start() and Shutdown() lifecycle.
* **Configuration Management**: Each wrapper has its own strongly-typed Config struct and helpers for loading configuration from files and environment variables.
* **Dependency Injection**: The wrappers are responsible for initializing all necessary clients (e.g., for Pub/Sub, GCS, Firestore) and injecting them into the core pipeline service.
* **HTTP Health Checks**: Each service automatically starts an HTTP server with standard /healthz and /readyz endpoints for cloud orchestration.

**Available Services:**

* **/ingestion**: A powerful MQTT-to-Pub/Sub bridge with dynamic, YAML-based routing.
* **/enrich**: A generic data enrichment service with a multi-layer cache (Redis \-\> Firestore).
* **/icestore**: A data archival service that batches messages and writes them to Google Cloud Storage.
* **/bigqueries**: A data sinking service that streams data into BigQuery.

## **End-to-End Testing Strategy (/e2e)**

The /e2e directory contains a suite of high-fidelity integration tests that validate the interactions between multiple live services and real cloud infrastructure. This provides the highest possible confidence before a production release.

The strategy is built on several core principles:

1. **High-Fidelity Environments**: E2E tests run against **real, deployed cloud resources** (Pub/Sub, BigQuery, GCS, Firestore). Emulators are used sparingly, only for components like MQTT brokers or Redis caches.
2. **Dynamic & Isolated Resources**: Each test run programmatically creates its own **unique, isolated set of cloud resources**. A random run\_ID is generated and appended to all resource names (e.g., ingestion-topic-a7b3c8f1). This prevents collisions between tests and ensures a clean state for every execution. All resources are automatically torn down at the end of the test.
3. **In-Process Service Orchestration**: The tests do not run compiled service binaries. Instead, they instantiate and run the service wrappers (e.g., ingestion.NewIngestionServiceWrapper) as **goroutines within the test process itself**. This provides precise control over the service lifecycle and makes it easy to capture logs and metrics directly.
4. **Programmatic Verification**: The outcome of a dataflow is verified by programmatically checking the state of the final data sink. The test suite includes verifier helpers that poll a GCS bucket