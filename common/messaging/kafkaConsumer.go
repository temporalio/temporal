package messaging

import (
	uberKafka "github.com/uber-go/kafka-client/kafka"

	"github.com/temporalio/temporal/common/log"
)

const rcvBufferSize = 2 * 1024

type (
	// a wrapper of uberKafka.Consumer to let the compiler happy
	kafkaConsumer struct {
		uConsumer uberKafka.Consumer
		logger    log.Logger
		msgC      chan Message
		doneC     chan struct{}
	}
)

var _ Consumer = (*kafkaConsumer)(nil)

func newKafkaConsumer(uConsumer uberKafka.Consumer, logger log.Logger) Consumer {
	return &kafkaConsumer{
		uConsumer: uConsumer,
		logger:    logger,
		msgC:      make(chan Message, rcvBufferSize),
		doneC:     make(chan struct{}),
	}
}

func (c *kafkaConsumer) Start() error {
	if err := c.uConsumer.Start(); err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-c.doneC:
				close(c.msgC)
				c.logger.Info("Stop consuming messages from channel")
				return
				// our Message interface is just a subset of Message interface in kafka-client so we don't need a wrapper here
			case uMsg := <-c.uConsumer.Messages():
				c.msgC <- uMsg
			}
		}
	}()
	return nil
}

// Stop stops the consumer
func (c *kafkaConsumer) Stop() {
	c.logger.Info("Stopping consumer")
	close(c.doneC)
	c.uConsumer.Stop()
}

// Messages return the message channel for this consumer
func (c *kafkaConsumer) Messages() <-chan Message {
	return c.msgC
}
