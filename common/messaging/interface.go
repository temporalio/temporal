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

import (
	"github.com/uber-go/kafka-client/kafka"

	"github.com/uber/cadence/.gen/go/replicator"
)

type (
	// Client is the interface used to abstract out interaction with messaging system for replication
	Client interface {
		NewConsumer(cadenceCluster, consumerName string, concurrency int) (kafka.Consumer, error)
		NewProducer(cadenceCluster string) (Producer, error)
	}

	// Producer is the interface used to send replication tasks to other clusters through replicator
	Producer interface {
		Publish(msg *replicator.ReplicationTask) error
		PublishBatch(msgs []*replicator.ReplicationTask) error
		Close() error
	}
)
