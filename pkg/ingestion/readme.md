# **Ingestion Service**

This package implements the primary entry point for data into the pipeline ecosystem. It is a microservice designed to connect to an MQTT broker, consume raw messages, perform initial in-flight transformations, and publish the standardized messages to a Google Cloud Pub/Sub topic for downstream services to consume.

This service cleverly combines components from the go-dataflow/pkg/mqttconverter and go-dataflow/pkg/enrichment packages to create a single, efficient pipeline.

## **Core Functionality**

* **Connects and Subscribes** to a specified MQTT broker and topic using the robust mqttconverter.MqttConsumer.
* **Performs Initial Enrichment**: As messages are consumed, they are passed through an enrichment.MessageEnricher function. This is used to perform initial, lightweight transformations, such as parsing the MQTT topic to extract a DeviceID and adding it to the message's EnrichmentData.
* **Publishes to Pub/Sub**: After the initial enrichment, the service publishes the standardized messagepipeline.MessageData to a configured output Pub/Sub topic.
* **Provides a Readiness Probe**: Exposes an HTTP /readyz endpoint that checks the underlying MQTT connection status, which is essential for Kubernetes and other orchestration systems.

## **How It Works**

This service demonstrates a powerful composition pattern. Instead of using a simple MessageTransformer, it uses the non-generic enrichment.EnrichmentService as its core processing engine. This might seem counterintuitive, but it's a smart choice for this use case:

1. The MqttConsumer is the data source.
2. An enrichment.MessageEnricher function is provided at startup. This function acts as the "transformer," but by operating on the message in-place, it's slightly more efficient as it avoids allocating a new struct for the transformed payload. It simply adds new keys to the EnrichmentData map.
3. The final MessageProcessor in the pipeline is a function that simply takes the now-enriched message and publishes it to Pub/Sub using a GooglePubsubProducer.

This architecture makes the service highly efficient for its task: consuming, lightly transforming, and forwarding data.

## **How to Use**

To use this service, you must instantiate the IngestionServiceWrapper and provide the initial enrichment logic.

### **Example: Initializing the Service**

1. **Define the initial enrichment logic** as a MessageEnricher.  
   // This enricher parses the topic "devices/DEVICE\_ID/data" to extract the ID.  
   deviceFinder := regexp.MustCompile(\`^\[^/\]+/(\[^/\]+)/\[^/\]+$\`)

   ingestionEnricher := func(ctx context.Context, msg \*messagepipeline.Message) (bool, error) {  
   topic := msg.Attributes\["mqtt\_topic"\]  
   var deviceID string

       matches := deviceFinder.FindStringSubmatch(topic)  
       if len(matches) \> 1 {  
           deviceID \= matches\[1\]  
       }

       if msg.EnrichmentData \== nil {  
           msg.EnrichmentData \= make(map\[string\]interface{})  
       }  
       msg.EnrichmentData\["DeviceID"\] \= deviceID  
       msg.EnrichmentData\["IngestedAt"\] \= time.Now().UTC()

       return false, nil // Continue processing  
   }

2. **Initialize and run the service** in your main.go.  
   // Load configuration  
   cfg := ingestion.LoadConfigDefaults("my-gcp-project")  
   cfg.MQTT.BrokerURL \= "tcp://mqtt.broker.com:1883"  
   cfg.MQTT.Topic \= "devices/+/data"  
   cfg.OutputTopicID \= "raw-messages-topic"

   // Create the service wrapper  
   wrapper, err := ingestion.NewIngestionServiceWrapper(  
   context.Background(),  
   cfg,  
   logger,  
   ingestionEnricher, // Inject the logic  
   )  
   if err \!= nil {  
   // handle error  
   }

   // Start the service and manage its lifecycle  
   // ...  
   wrapper.Start(ctx)  
   // ...  
   wrapper.Shutdown(ctx)

## **Configuration**

The service is configured via the Config struct in inconfig.go. Key settings include:

* ProjectID: Your Google Cloud project ID.
* MQTT: The full MQTTClientConfig for connecting to the broker.
* OutputTopicID: The Pub/Sub topic to which raw messages will be published.
* NumWorkers: The number of concurrent goroutines processing messages from MQTT.
* BufferSize: The size of the in-memory channel between the MQTT consumer and the processing workers.