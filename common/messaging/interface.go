// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package messaging

type (
	// Client is the interface used to abstract out interaction with messaging system for replication
	Client interface {
		NewConsumer(currentCluster, sourceCluster, consumerName string, concurrency int) (Consumer, error)
		NewProducer(topic string) (Producer, error)
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
		//PublishBatch(msgs []*replicator.ReplicationTask) error
		PublishBatch(msgs []interface{}) error
		Publish(msgs interface{}) error
		Close() error
	}
)
