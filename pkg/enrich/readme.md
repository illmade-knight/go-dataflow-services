# **Enrichment Microservice**

This package implements a generic, reusable microservice for enriching data streams. It consumes messages from an input Pub/Sub topic, adds contextual data by fetching it from an external source (via a cache), and then publishes the enriched message to an output Pub/Sub topic.

This service is a practical implementation of the patterns provided by the go-dataflow/pkg/enrichment and go-dataflow/pkg/cache packages.

## **Core Functionality**

* **Consumes** messages from an input Pub/Sub topic.
* **Unwraps** messages that are already in the canonical MessageData format from an upstream service.
* **Enriches** each message by using a key from the message to fetch data from a high-performance, multi-layered cache.
* **Applies** the fetched data to the message's EnrichmentData map.
* **Publishes** the newly enriched MessageData to an output Pub/Sub topic.
* **Manages** the full message lifecycle, including acknowledgments and graceful shutdown of all components.

## **How It Works**

This service is built around the enrichment.EnrichmentService. The core logic is injected upon creation, making the service highly flexible and testable. The NewEnrichmentServiceWrapper constructor requires three key components to be provided:

1. A **cache.Fetcher\[K, V\]**: This is the top-level entry point to the caching chain (e.g., InMemoryLRUCache \-\> RedisCache \-\> Firestore). The service will call fetcher.Fetch() to get the enrichment data.
2. An **enrichment.KeyExtractor\[K\]**: A function that knows how to extract the lookup key from an incoming messagepipeline.Message.
3. An **enrichment.Applier\[V\]**: A function that knows how to apply the data returned by the fetcher to the message's EnrichmentData map.

By depending on these interfaces and functions, the service's core pipeline logic remains completely decoupled from the specific data types or caching strategies being used.

## **How to Use**

To use this service, you must instantiate the EnrichmentServiceWrapper with concrete types and provide the necessary fetcher and functions.

### **Example: Enriching with DeviceInfo**

1. **Define your enrichment data structure.**  
   type DeviceInfo struct {  
   ClientID   string  
   LocationID string  
   }

2. **Define the KeyExtractor and Applier functions.**  
   // Extracts the deviceID from message attributes  
   keyExtractor := func(msg \*messagepipeline.Message) (string, bool) {  
   id, ok := msg.Attributes\["deviceID"\]  
   return id, ok  
   }

   // Applies the fetched DeviceInfo to the message  
   applier := func(msg \*messagepipeline.Message, data DeviceInfo) {  
   if msg.EnrichmentData \== nil {  
   msg.EnrichmentData \= make(map\[string\]interface{})  
   }  
   msg.EnrichmentData\["client"\] \= data.ClientID  
   msg.EnrichmentData\["location"\] \= data.LocationID  
   }

3. **Assemble the caching chain** using the cache package.  
   // In a real app, you'd create Firestore and Redis clients here.  
   firestoreFetcher, \_ := cache.NewFirestore\[string, DeviceInfo\](...)  
   redisCache, \_ := cache.NewRedisCache\[string, DeviceInfo\](..., firestoreFetcher)  
   lruCache, \_ := cache.NewInMemoryLRUCache\[string, DeviceInfo\](1000, redisCache)

4. **Initialize and run the service** in your main.go.  
   // Load configuration  
   cfg := enrich.LoadConfigDefaults("my-gcp-project")  
   cfg.InputSubscriptionID \= "raw-messages-sub"  
   cfg.OutputTopicID \= "enriched-messages-topic"

   // Create the service wrapper, injecting the top-level cache and functions  
   wrapper, err := enrich.NewEnrichmentServiceWrapper\[string, DeviceInfo\](  
   context.Background(),  
   cfg,  
   logger,  
   lruCache, // Inject the L1 cache  
   keyExtractor,  
   applier,  
   )  
   if err \!= nil {  
   // handle error  
   }

   // Start and manage the service lifecycle  
   // ...

## **Configuration**

The service is configured via the Config struct in enconfig.go. Key settings include:

* ProjectID: Your Google Cloud project ID.
* InputSubscriptionID: The source Pub/Sub subscription.
* OutputTopicID: The destination Pub/Sub topic for enriched messages.
* CacheConfig: Contains RedisConfig and FirestoreConfig for setting up the caching layers.
* NumWorkers: The number of concurrent workers processing messages.