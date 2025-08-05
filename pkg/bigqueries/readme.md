# **BigQuery Sinking Service**

This package implements a generic, reusable microservice designed to consume messages from a Google Cloud Pub/Sub topic and efficiently sink them into a Google BigQuery table.

It is built on top of the go-dataflow/pkg/bqstore and go-dataflow/pkg/messagepipeline packages, abstracting away the boilerplate of setting up a batch-processing pipeline for BigQuery.

## **Core Functionality**

* **Consumes** messages from a specified Pub/Sub subscription.
* **Transforms** each message from its raw format into a specific, structured Go type suitable for a BigQuery table schema.
* **Batches** the transformed records efficiently based on size or time intervals.
* **Streams** the batches into the target BigQuery table using the high-throughput Storage Write API.
* **Handles** acknowledgments (Ack/Nack) automatically. A batch is only acknowledged after it has been successfully inserted into BigQuery.

## **How It Works**

The service is essentially a wrapper around the bqstore.NewBigQueryService. The NewBQServiceWrapper constructor handles the initialization of all necessary components:

1. A GooglePubsubConsumer to pull messages from the configured subscription.
2. A BigQueryInserter to handle the streaming writes to the target table. It will even attempt to create the table with an inferred schema if it doesn't exist.
3. A messagepipeline.BatchingService that orchestrates the entire flow.

The key to its generic nature is the messagepipeline.MessageTransformer function. This function is provided during the service's initialization and contains the specific logic for converting a raw messagepipeline.Message into the target data type T that matches the BigQuery schema.

## **How to Use**

To use this service, you need to instantiate the BQServiceWrapper with a concrete type and provide a transformer for that type.

### **Example: Sinking TestPayload Structs**

1. **Define your data structure** with BigQuery tags.  
   type TestPayload struct {  
   MessageID string    \`bigquery:"message\_id"\`  
   Data      string    \`bigquery:"data"\`  
   Timestamp time.Time \`bigquery:"timestamp"\`  
   }

2. **Define a MessageTransformer** for this type.  
   transformer := func(ctx context.Context, msg \*messagepipeline.Message) (\*TestPayload, bool, error) {  
   // This is where you parse msg.Payload and create the struct.  
   return \&TestPayload{  
   MessageID: msg.ID,  
   Data:      string(msg.Payload),  
   Timestamp: msg.PublishTime,  
   }, false, nil  
   }

3. **Initialize and run the service** in your main.go.  
   // Load configuration  
   cfg := bigqueries.LoadConfigDefaults("my-gcp-project")  
   cfg.InputSubscriptionID \= "my-input-sub"  
   cfg.BigQueryConfig.DatasetID \= "my\_dataset"  
   cfg.BigQueryConfig.TableID \= "my\_table"

   // Create the service wrapper  
   wrapper, err := bigqueries.NewBQServiceWrapper\[TestPayload\](  
   context.Background(),   
   cfg,   
   logger,   
   transformer,  
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

The service is configured via the Config struct in bqconfig.go. Key settings include:

* ProjectID: Your Google Cloud project ID.
* InputSubscriptionID: The Pub/Sub subscription to pull messages from.
* BigQueryConfig: The target DatasetID and TableID.
* BatchProcessing: Settings for the batching engine, such as NumWorkers, BatchSize, and FlushInterval.