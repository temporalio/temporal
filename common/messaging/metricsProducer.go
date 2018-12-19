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
	"github.com/uber/cadence/common/metrics"
)

type (
	metricsProducer struct {
		producer      Producer
		metricsClient metrics.Client
	}
)

// NewMetricProducer creates a new instance of producer that emits metrics
func NewMetricProducer(producer Producer,
	metricsClient metrics.Client) Producer {
	return &metricsProducer{
		producer:      producer,
		metricsClient: metricsClient,
	}
}

func (p *metricsProducer) PublishBatch(msgs []interface{}) error {
	p.metricsClient.IncCounter(metrics.MessagingClientPublishBatchScope, metrics.CadenceClientRequests)

	sw := p.metricsClient.StartTimer(metrics.MessagingClientPublishBatchScope, metrics.CadenceClientLatency)
	err := p.producer.PublishBatch(msgs)
	sw.Stop()

	if err != nil {
		p.metricsClient.IncCounter(metrics.MessagingClientPublishBatchScope, metrics.CadenceClientFailures)
	}
	return err
}

func (p *metricsProducer) Publish(msg interface{}) error {
	p.metricsClient.IncCounter(metrics.MessagingClientPublishScope, metrics.CadenceClientRequests)

	sw := p.metricsClient.StartTimer(metrics.MessagingClientPublishScope, metrics.CadenceClientLatency)
	err := p.producer.Publish(msg)
	sw.Stop()

	if err != nil {
		p.metricsClient.IncCounter(metrics.MessagingClientPublishScope, metrics.CadenceClientFailures)
	}
	return err
}

func (p *metricsProducer) Close() error {
	return p.producer.Close()
}
