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
	"fmt"
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
		ClusterToTopic map[string]TopicList     `yaml:"cadence-cluster-topics"`
	}

	// ClusterConfig describes the configuration for a single Kafka cluster
	ClusterConfig struct {
		Brokers []string `yaml:"brokers"`
	}

	// TopicConfig describes the mapping from topic to Kafka cluster
	TopicConfig struct {
		Cluster string `yaml:"cluster"`
	}

	// TopicList describes the topic names for each cluster
	TopicList struct {
		Topic      string `yaml:"topic"`
		RetryTopic string `yaml:"retry-topic"`
		DLQTopic   string `yaml:"dlq-topic"`
	}
)

// NewKafkaClient is used to create an instance of KafkaClient
func (k *KafkaConfig) NewKafkaClient(zLogger *zap.Logger, logger bark.Logger, metricScope tally.Scope) Client {
	k.validate()

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

func (k *KafkaConfig) validate() {
	if len(k.Clusters) == 0 {
		panic("Empty Kafka Cluster Config")
	}
	if len(k.Topics) == 0 {
		panic("Empty Topics Config")
	}
	if len(k.ClusterToTopic) == 0 {
		panic("Empty Cluster To Topics Config")
	}

	validateTopicsFn := func(topic string) {
		if topic == "" {
			panic("Empty Topic Name")
		} else if topicConfig, ok := k.Topics[topic]; !ok {
			panic(fmt.Sprintf("Missing Topic Config for Topic %v", topic))
		} else if clusterConfig, ok := k.Clusters[topicConfig.Cluster]; !ok {
			panic(fmt.Sprintf("Missing Kafka Cluster Config for Cluster %v", topicConfig.Cluster))
		} else if len(clusterConfig.Brokers) == 0 {
			panic(fmt.Sprintf("Missing Kafka Brokers Config for Cluster %v", topicConfig.Cluster))
		}
	}

	for _, topics := range k.ClusterToTopic {
		validateTopicsFn(topics.Topic)
		validateTopicsFn(topics.RetryTopic)
		validateTopicsFn(topics.DLQTopic)
	}
}

func (k *KafkaConfig) getTopicsForCadenceCluster(cadenceCluster string) TopicList {
	return k.ClusterToTopic[cadenceCluster]
}

func (k *KafkaConfig) getKafkaClusterForTopic(topic string) string {
	return k.Topics[topic].Cluster
}

func (k *KafkaConfig) getBrokersForKafkaCluster(kafkaCluster string) []string {
	return k.Clusters[kafkaCluster].Brokers
}
