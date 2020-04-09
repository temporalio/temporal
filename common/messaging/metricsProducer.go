package messaging

import (
	"github.com/temporalio/temporal/common/metrics"
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

func (p *metricsProducer) Publish(msg interface{}) error {
	p.metricsClient.IncCounter(metrics.MessagingClientPublishScope, metrics.ClientRequests)

	sw := p.metricsClient.StartTimer(metrics.MessagingClientPublishScope, metrics.ClientLatency)
	err := p.producer.Publish(msg)
	sw.Stop()

	if err != nil {
		p.metricsClient.IncCounter(metrics.MessagingClientPublishScope, metrics.ClientFailures)
	}
	return err
}

func (p *metricsProducer) Close() error {
	if closeableProducer, ok := p.producer.(CloseableProducer); ok {
		return closeableProducer.Close()
	}

	return nil
}
