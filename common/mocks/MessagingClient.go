// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package mocks

import (
	"go.temporal.io/server/common/messaging"
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
