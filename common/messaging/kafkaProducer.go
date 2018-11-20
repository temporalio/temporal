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
	"errors"
	"github.com/Shopify/sarama"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/codec/gob"
	"github.com/uber/cadence/common/logging"
)

type (
	kafkaProducer struct {
		topic      string
		producer   sarama.SyncProducer
		msgEncoder codec.BinaryEncoder
		gobEncoder *gob.Encoder
		logger     bark.Logger
	}
)

var _ Producer = (*kafkaProducer)(nil)

// NewKafkaProducer is used to create the Kafka based producer implementation
func NewKafkaProducer(topic string, producer sarama.SyncProducer, logger bark.Logger) Producer {
	return &kafkaProducer{
		topic:      topic,
		producer:   producer,
		msgEncoder: codec.NewThriftRWEncoder(),
		gobEncoder: gob.NewGobEncoder(),
		logger: logger.WithFields(bark.Fields{
			logging.TagTopicName: topic,
		}),
	}
}

// Publish is used to send messages to other clusters through Kafka topic
func (p *kafkaProducer) Publish(msg interface{}) error {
	message, err := p.getProducerMessage(msg)
	if err != nil {
		return err
	}

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		p.logger.WithFields(bark.Fields{
			logging.TagPartition:    partition,
			logging.TagPartitionKey: message.Key,
			logging.TagOffset:       offset,
			logging.TagErr:          err,
		}).Warn("Failed to publish message to kafka")
		return err
	}

	return nil
}

// PublishBatch is used to send messages to other clusters through Kafka topic
func (p *kafkaProducer) PublishBatch(msgs []interface{}) error {
	var producerMsgs []*sarama.ProducerMessage
	for _, msg := range msgs {
		message, err := p.getProducerMessage(msg)
		if err != nil {
			return err
		}
		producerMsgs = append(producerMsgs, message)
	}

	err := p.producer.SendMessages(producerMsgs)
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
	case replicator.ReplicationTaskTypeSyncActivity:
		// Use workflowID as the partition key so all sync activity tasks for a workflow are dispatched to the same
		// Kafka partition.  This will give us some ordering guarantee for workflow replication tasks atleast at
		// the messaging layer perspective
		attributes := task.SyncActicvityTaskAttributes
		return sarama.StringEncoder(attributes.GetWorkflowId())
	}

	return nil
}

func (p *kafkaProducer) getProducerMessage(message interface{}) (*sarama.ProducerMessage, error) {
	switch message.(type) {
	case *replicator.ReplicationTask:
		task := message.(*replicator.ReplicationTask)
		payload, err := p.serializeTask(task)
		if err != nil {
			return nil, err
		}
		partitionKey := p.getKey(task)
		msg := &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   partitionKey,
			Value: sarama.ByteEncoder(payload),
		}
		return msg, nil
	case *OpenWorkflowMsg:
		openRecord := message.(*OpenWorkflowMsg)
		payload, err := p.gobEncoder.Encode(openRecord)
		if err != nil {
			return nil, err
		}
		msg := &sarama.ProducerMessage{
			Topic: VisibilityTopicName,
			Key:   sarama.StringEncoder(openRecord.WorkflowID),
			Value: sarama.ByteEncoder(payload),
		}
		return msg, nil
	default:
		return nil, errors.New("unknown producer message type")
	}
}
