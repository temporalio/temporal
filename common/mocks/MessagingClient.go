package mocks

import (
	"github.com/temporalio/temporal/common/messaging"
)

type (
	// MessagingClient is the mock implementation for Service interface
	MessagingClient struct {
		consumerMock  messaging.Consumer
		publisherMock messaging.Producer
	}
)

var _ messaging.Client = (*MessagingClient)(nil)

// NewMockMessagingClient generate a dummy implementation of messaging client
func NewMockMessagingClient(publisher messaging.Producer, consumer messaging.Consumer) messaging.Client {
	return &MessagingClient{
		publisherMock: publisher,
		consumerMock:  consumer,
	}
}

// NewConsumer generates a dummy implementation of kafka consumer
func (c *MessagingClient) NewConsumer(appName, consumerName string, concurrency int) (messaging.Consumer, error) {
	return c.consumerMock, nil
}

// NewConsumerWithClusterName generates a dummy implementation of kafka consumer
func (c *MessagingClient) NewConsumerWithClusterName(currentCluster, sourceCluster, consumerName string, concurrency int) (messaging.Consumer, error) {
	return c.consumerMock, nil
}

// NewProducer generates a dummy implementation of kafka producer
func (c *MessagingClient) NewProducer(appName string) (messaging.Producer, error) {
	return c.publisherMock, nil
}

// NewProducerWithClusterName generates a dummy implementation of kafka producer
func (c *MessagingClient) NewProducerWithClusterName(sourceCluster string) (messaging.Producer, error) {
	return c.publisherMock, nil
}
