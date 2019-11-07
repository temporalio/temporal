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

	"github.com/uber/cadence/common/auth"
)

type (
	// KafkaConfig describes the configuration needed to connect to all kafka clusters
	KafkaConfig struct {
		TLS            auth.TLS                 `yaml:"tls"`
		Clusters       map[string]ClusterConfig `yaml:"clusters"`
		Topics         map[string]TopicConfig   `yaml:"topics"`
		ClusterToTopic map[string]TopicList     `yaml:"cadence-cluster-topics"`
		Applications   map[string]TopicList     `yaml:"applications"`
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

// Validate will validate config for kafka
func (k *KafkaConfig) Validate(checkCluster bool, checkApp bool) {
	if len(k.Clusters) == 0 {
		panic("Empty Kafka Cluster Config")
	}
	if len(k.Topics) == 0 {
		panic("Empty Topics Config")
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

	if checkCluster {
		if len(k.ClusterToTopic) == 0 {
			panic("Empty Cluster To Topics Config")
		}
		for _, topics := range k.ClusterToTopic {
			validateTopicsFn(topics.Topic)
			validateTopicsFn(topics.DLQTopic)
		}
	}
	if checkApp {
		if len(k.Applications) == 0 {
			panic("Empty Applications Config")
		}
		for _, topics := range k.Applications {
			validateTopicsFn(topics.Topic)
			validateTopicsFn(topics.DLQTopic)
		}
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

func (k *KafkaConfig) getTopicsForApplication(app string) TopicList {
	return k.Applications[app]
}
