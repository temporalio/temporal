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
	uberKafka "github.com/uber-go/kafka-client/kafka"

	"github.com/uber/cadence/common/log"
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
