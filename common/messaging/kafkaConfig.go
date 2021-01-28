// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

	"go.temporal.io/server/common/auth"
)

type (
	// KafkaConfig describes the configuration needed to connect to all kafka clusters
	KafkaConfig struct {
		TLS            auth.TLS                 `yaml:"tls"`
		SASL           SASLConfig               `yaml:"sasl"`
		Clusters       map[string]ClusterConfig `yaml:"clusters"`
		Topics         map[string]TopicConfig   `yaml:"topics"`
		ClusterToTopic map[string]TopicList     `yaml:"temporal-cluster-topics"`
		Applications   map[string]TopicList     `yaml:"applications"`
	}

	// ClusterConfig describes the configuration for a single Kafka cluster
	ClusterConfig struct {
		Brokers []string `yaml:"brokers"`
	}

	// SASLConfig describes the configuration for connecting via SASL for a single Kafka cluster
	SASLConfig struct {
		Enabled  bool   `yaml:"enabled"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`

		// Mechanism only supports the following options: scramsha256 or scramsha512
		Mechanism string `yaml:"mechanism"`
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
func (k *KafkaConfig) Validate(checkCluster bool, checkApp bool) error {
	if len(k.Clusters) == 0 {
		return fmt.Errorf("empty Kafka cluster config")
	}
	if len(k.Topics) == 0 {
		return fmt.Errorf("empty topics config")
	}

	validateTopicsFn := func(topic string) error {
		if topic == "" {
			return fmt.Errorf("empty topic name")
		} else if topicConfig, ok := k.Topics[topic]; !ok {
			return fmt.Errorf("missing topic config for topic %v", topic)
		} else if clusterConfig, ok := k.Clusters[topicConfig.Cluster]; !ok {
			return fmt.Errorf("missing Kafka cluster config for cluster %v", topicConfig.Cluster)
		} else if len(clusterConfig.Brokers) == 0 {
			return fmt.Errorf("missing Kafka brokers config for cluster %v", topicConfig.Cluster)
		}
		return nil
	}

	if checkCluster {
		if len(k.ClusterToTopic) == 0 {
			return fmt.Errorf("empty cluster to topics config")
		}
		for _, topics := range k.ClusterToTopic {
			err := validateTopicsFn(topics.Topic)
			if err != nil {
				return err
			}
			err = validateTopicsFn(topics.DLQTopic)
			if err != nil {
				return err
			}
		}
	}
	if checkApp {
		if len(k.Applications) == 0 {
			return fmt.Errorf("empty applications config")
		}
		for _, topics := range k.Applications {
			err := validateTopicsFn(topics.Topic)
			if err != nil {
				return err
			}
			err = validateTopicsFn(topics.DLQTopic)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (k *KafkaConfig) getTopicsForTemporalCluster(temporalCluster string) TopicList {
	return k.ClusterToTopic[temporalCluster]
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
