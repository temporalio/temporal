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
	"github.com/Shopify/sarama"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/logging"

	"github.com/uber/cadence/.gen/go/replicator"
)

type (
	kafkaProducer struct {
		topic      string
		producer   sarama.SyncProducer
		msgEncoder codec.BinaryEncoder
		logger     bark.Logger
	}
)

// NewKafkaProducer is used to create the Kafka based producer implementation
func NewKafkaProducer(topic string, producer sarama.SyncProducer, logger bark.Logger) Producer {
	return &kafkaProducer{
		topic:      topic,
		producer:   producer,
		msgEncoder: codec.NewThriftRWEncoder(),
		logger: logger.WithFields(bark.Fields{
			logging.TagTopicName: topic,
		}),
	}
}

// Publish is used to send messages to other clusters through Kafka topic
func (p *kafkaProducer) Publish(task *replicator.ReplicationTask) error {
	payload, err := p.serializeTask(task)
	if err != nil {
		return err
	}

	partitionKey := p.getKey(task)

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   partitionKey,
		Value: sarama.ByteEncoder(payload),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		p.logger.WithFields(bark.Fields{
			logging.TagPartition:    partition,
			logging.TagPartitionKey: partitionKey,
			logging.TagOffset:       offset,
			logging.TagErr:          err,
		}).Warn("Failed to publish message to kafka")

		return err
	}

	return nil
}

// PublishBatch is used to send messages to other clusters through Kafka topic
func (p *kafkaProducer) PublishBatch(tasks []*replicator.ReplicationTask) error {
	var msgs []*sarama.ProducerMessage
	for _, task := range tasks {
		payload, err := p.serializeTask(task)
		if err != nil {
			return err
		}

		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: p.topic,
			Value: sarama.ByteEncoder(payload),
		})
	}

	err := p.producer.SendMessages(msgs)
	if err != nil {
		p.logger.WithFields(bark.Fields{
			logging.TagErr: err,
		}).Warn("Failed to publish batch of messages to kafka")

		return err
	}

	return nil
}

// Close is used to close Kafka publisher
func (p *kafkaProducer) Close() error {
	return p.producer.Close()
}

func (p *kafkaProducer) serializeTask(task *replicator.ReplicationTask) ([]byte, error) {
	payload, err := p.msgEncoder.Encode(task)
	if err != nil {
		p.logger.WithFields(bark.Fields{
			logging.TagErr: err,
		}).Error("Failed to serialize replication task")

		return nil, err
	}

	return payload, nil
}

func (p *kafkaProducer) getKey(task *replicator.ReplicationTask) sarama.Encoder {
	if task == nil {
		return nil
	}

	switch task.GetTaskType() {
	case replicator.ReplicationTaskTypeHistory:
		// Use workflowID as the partition key so all replication tasks for a workflow are dispatched to the same
		// Kafka partition.  This will give us some ordering guarantee for workflow replication tasks atleast at
		// the messaging layer perspective
		attributes := task.HistoryTaskAttributes
		return sarama.StringEncoder(attributes.GetWorkflowId())
	}

	return nil
}
