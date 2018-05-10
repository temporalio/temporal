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

	"github.com/uber-common/bark"
	"github.com/uber-go/kafka-client"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	// KafkaConfig describes the configuration needed to connect to all kafka clusters
	KafkaConfig struct {
		Clusters       map[string]ClusterConfig `yaml:"clusters"`
		Topics         map[string]TopicConfig   `yaml:"topics"`
		ClusterToTopic map[string]string        `yaml:"cadence-cluster-to-topic"`
	}

	// ClusterConfig describes the configuration for a single Kafka cluster
	ClusterConfig struct {
		Brokers []string `yaml:"brokers"`
	}

	// TopicConfig describes the mapping from topic to Kafka cluster
	TopicConfig struct {
		Cluster string `yaml:"cluster"`
	}
)

// NewKafkaClient is used to create an instance of KafkaClient
func (k *KafkaConfig) NewKafkaClient(zLogger *zap.Logger, logger bark.Logger, metricScope tally.Scope) Client {
	// mapping from cluster name to list of broker ip addresses
	brokers := map[string][]string{}
	for cluster, cfg := range k.Clusters {
		brokers[cluster] = cfg.Brokers
		for i := range brokers[cluster] {
			if !strings.Contains(cfg.Brokers[i], ":") {
				cfg.Brokers[i] += ":9092"
			}
		}
	}

	// mapping from topic name to cluster that has that topic
	topicClusterAssignment := map[string][]string{}
	for topic, cfg := range k.Topics {
		topicClusterAssignment[topic] = []string{cfg.Cluster}
	}

	client := kafkaclient.New(kafka.NewStaticNameResolver(topicClusterAssignment, brokers), zLogger, metricScope)

	return &kafkaClient{
		config: k,
		client: client,
		logger: logger,
	}
}

func (k *KafkaConfig) getTopicForCadenceCluster(cluster string) string {
	return k.ClusterToTopic[cluster]
}

func (k *KafkaConfig) getKafkaClusterForTopic(topic string) string {
	return k.Topics[topic].Cluster
}

func (k *KafkaConfig) getBrokersForKafkaCluster(cluster string) []string {
	return k.Clusters[cluster].Brokers
}
