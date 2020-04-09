package messaging

type (
	// Client is the interface used to abstract out interaction with messaging system for replication
	Client interface {
		NewConsumer(appName, consumerName string, concurrency int) (Consumer, error)
		NewConsumerWithClusterName(currentCluster, sourceCluster, consumerName string, concurrency int) (Consumer, error)
		NewProducer(appName string) (Producer, error)
		NewProducerWithClusterName(sourceCluster string) (Producer, error)
	}

	// Consumer is the unified interface for both internal and external kafka clients
	Consumer interface {
		// Start starts the consumer
		Start() error
		// Stop stops the consumer
		Stop()
		// Messages return the message channel for this consumer
		Messages() <-chan Message
	}

	// Message is the unified interface for a Kafka message
	Message interface {
		// Value is a mutable reference to the message's value
		Value() []byte
		// Partition is the ID of the partition from which the message was read.
		Partition() int32
		// Offset is the message's offset.
		Offset() int64
		// Ack marks the message as successfully processed.
		Ack() error
		// Nack marks the message processing as failed and the message will be retried or sent to DLQ.
		Nack() error
	}

	// Producer is the interface used to send replication tasks to other clusters through replicator
	Producer interface {
		Publish(message interface{}) error
	}

	// CloseableProducer is a Producer that can be closed
	CloseableProducer interface {
		Producer
		Close() error
	}
)
