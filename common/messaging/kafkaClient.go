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
	"strings"

	"github.com/Shopify/sarama"
	"github.com/uber-common/bark"
	"github.com/uber-go/kafka-client"
	"github.com/uber-go/kafka-client/kafka"
)

type (
	kafkaClient struct {
		config *KafkaConfig
		client *kafkaclient.Client
		logger bark.Logger
	}
)

// NewConsumer is used to create a Kafka consumer
func (c *kafkaClient) NewConsumer(cadenceCluster, consumerName string, concurrency int) (kafka.Consumer, error) {
	topicName := c.config.getTopicForCadenceCluster(cadenceCluster)
	kafkaClusterName := c.config.getKafkaClusterForTopic(topicName)
	brokers := c.config.getBrokersForKafkaCluster(kafkaClusterName)

	consumerConfig := &kafka.ConsumerConfig{
		GroupName: consumerName,
		TopicList: kafka.ConsumerTopicList{
			kafka.ConsumerTopic{
				Topic: kafka.Topic{
					Name:       topicName,
					Cluster:    kafkaClusterName,
					BrokerList: brokers,
				},
				RetryQ: kafka.Topic{
					Name:       strings.Join([]string{topicName, "retry"}, "-"),
					Cluster:    kafkaClusterName,
					BrokerList: brokers,
				},
				DLQ: kafka.Topic{
					Name:       strings.Join([]string{topicName, "dlq"}, "-"),
					Cluster:    kafkaClusterName,
					BrokerList: brokers,
				},
			},
		},
		Concurrency: concurrency,
	}

	consumer, err := c.client.NewConsumer(consumerConfig)
	return consumer, err
}

// NewProducer is used to create a Kafka producer for shipping replication tasks
func (c *kafkaClient) NewProducer(cadenceCluster string) (Producer, error) {
	topicName := c.config.getTopicForCadenceCluster(cadenceCluster)
	kafkaClusterName := c.config.getKafkaClusterForTopic(topicName)
	brokers := c.config.getBrokersForKafkaCluster(kafkaClusterName)

	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return nil, err
	}

	return NewKafkaProducer(topicName, producer, c.logger), nil
}
