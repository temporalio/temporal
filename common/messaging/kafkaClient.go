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
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"io/ioutil"
	"strings"

	"go.temporal.io/server/common/auth"

	"github.com/Shopify/sarama"
	temporalKafkaClient "github.com/temporalio/kafka-client"
	temporalKafka "github.com/temporalio/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/xdg/scram"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	// This is a default implementation of Client interface which makes use of temporalio/kafka-client as consumer
	kafkaClient struct {
		config        *KafkaConfig
		tlsConfig     *tls.Config
		client        temporalKafkaClient.Client
		metricsClient metrics.Client
		logger        log.Logger
	}
)

var _ Client = (*kafkaClient)(nil)

// NewKafkaClient is used to create an instance of KafkaClient
func NewKafkaClient(kc *KafkaConfig, metricsClient metrics.Client, zLogger *zap.Logger, logger log.Logger, metricScope tally.Scope,
	checkCluster, checkApp bool) Client {
	kc.Validate(checkCluster, checkApp)

	// mapping from cluster name to list of broker ip addresses
	brokers := map[string][]string{}
	for cluster, cfg := range kc.Clusters {
		brokers[cluster] = cfg.Brokers
		for i := range brokers[cluster] {
			if !strings.Contains(cfg.Brokers[i], ":") {
				cfg.Brokers[i] += ":9092"
			}
		}
	}

	// mapping from topic name to cluster that has that topic
	topicClusterAssignment := map[string][]string{}
	for topic, cfg := range kc.Topics {
		topicClusterAssignment[topic] = []string{cfg.Cluster}
	}

	client := temporalKafkaClient.New(temporalKafka.NewStaticNameResolver(topicClusterAssignment, brokers), zLogger, metricScope)

	tlsConfig, err := CreateTLSConfig(kc.TLS)
	if err != nil {
		panic(fmt.Sprintf("Error creating Kafka TLS config %v", err))
	}

	return &kafkaClient{
		config:        kc,
		tlsConfig:     tlsConfig,
		client:        client,
		metricsClient: metricsClient,
		logger:        logger,
	}
}

// NewConsumer is used to create a Kafka consumer
func (c *kafkaClient) NewConsumer(app, consumerName string, concurrency int) (Consumer, error) {
	topics := c.config.getTopicsForApplication(app)
	kafkaClusterNameForTopic := c.config.getKafkaClusterForTopic(topics.Topic)
	kafkaClusterNameForDLQTopic := c.config.getKafkaClusterForTopic(topics.DLQTopic)

	topic := createTemporalKafkaTopic(topics.Topic, kafkaClusterNameForTopic)
	dlq := createTemporalKafkaTopic(topics.DLQTopic, kafkaClusterNameForDLQTopic)

	return c.newConsumerHelper(topic, dlq, consumerName, concurrency)
}

// NewConsumerWithClusterName is used to create a Kafka consumer for consuming replication tasks
func (c *kafkaClient) NewConsumerWithClusterName(currentCluster, sourceCluster, consumerName string, concurrency int) (Consumer, error) {
	currentTopics := c.config.getTopicsForTemporalCluster(currentCluster)
	sourceTopics := c.config.getTopicsForTemporalCluster(sourceCluster)
	kafkaClusterNameForTopic := c.config.getKafkaClusterForTopic(sourceTopics.Topic)
	kafkaClusterNameForDLQTopic := c.config.getKafkaClusterForTopic(currentTopics.DLQTopic)

	topic := createTemporalKafkaTopic(sourceTopics.Topic, kafkaClusterNameForTopic)
	dlq := createTemporalKafkaTopic(currentTopics.DLQTopic, kafkaClusterNameForDLQTopic)

	return c.newConsumerHelper(topic, dlq, consumerName, concurrency)
}

func createTemporalKafkaTopic(name, cluster string) *temporalKafka.Topic {
	return &temporalKafka.Topic{
		Name:    name,
		Cluster: cluster,
	}
}

func (c *kafkaClient) newConsumerHelper(topic, dlq *temporalKafka.Topic, consumerName string, concurrency int) (Consumer, error) {
	topicList := temporalKafka.ConsumerTopicList{
		temporalKafka.ConsumerTopic{
			Topic: *topic,
			DLQ:   *dlq,
		},
	}
	consumerConfig := temporalKafka.NewConsumerConfig(consumerName, topicList)
	consumerConfig.Concurrency = concurrency
	consumerConfig.Offsets.Initial.Offset = temporalKafka.OffsetOldest
	consumerConfig.TLSConfig = c.tlsConfig

	options := []temporalKafkaClient.ConsumerOption{}

	if c.config.SASL.Enabled {
		options = append(options, temporalKafkaClient.WithSASLMechanism(
			c.config.SASL.User,
			c.config.SASL.Password,
			c.config.SASL.Mechanism,
		))
	}

	uConsumer, err := c.client.NewConsumer(consumerConfig, options...)
	if err != nil {
		return nil, err
	}
	return newKafkaConsumer(uConsumer, c.logger), nil
}

// NewProducer is used to create a Kafka producer
func (c *kafkaClient) NewProducer(app string) (Producer, error) {
	topics := c.config.getTopicsForApplication(app)
	return c.newProducerHelper(topics.Topic)
}

// NewProducerWithClusterName is used to create a Kafka producer for shipping replication tasks
func (c *kafkaClient) NewProducerWithClusterName(sourceCluster string) (Producer, error) {
	topics := c.config.getTopicsForTemporalCluster(sourceCluster)
	return c.newProducerHelper(topics.Topic)
}

type scramClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (s *scramClient) Begin(userName, password, authzID string) (err error) {
	s.Client, err = s.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	s.ClientConversation = s.Client.NewConversation()
	return nil
}

func (s *scramClient) Step(challenge string) (response string, err error) {
	response, err = s.ClientConversation.Step(challenge)
	return
}

func (s *scramClient) Done() bool {
	return s.ClientConversation.Done()
}

func (c *kafkaClient) newProducerHelper(topic string) (Producer, error) {
	kafkaClusterName := c.config.getKafkaClusterForTopic(topic)
	brokers := c.config.getBrokersForKafkaCluster(kafkaClusterName)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = c.tlsConfig != nil
	config.Net.TLS.Config = c.tlsConfig

	if c.config.SASL.Enabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.config.SASL.User
		config.Net.SASL.Password = c.config.SASL.Password

		switch strings.ToLower(c.config.SASL.Mechanism) {
		case "scramsha256":
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &scramClient{HashGeneratorFcn: func() hash.Hash { return sha256.New() }}
			}
		case "scramsha512":
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &scramClient{HashGeneratorFcn: func() hash.Hash { return sha512.New() }}
			}
		default:
			return nil, fmt.Errorf("unknown sasl mechanism specified: %+v", c.config.SASL.Mechanism)
		}
	}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	if c.metricsClient != nil {
		c.logger.Info("Create producer with metricsClient")
		return NewMetricProducer(NewKafkaProducer(topic, producer, c.logger), c.metricsClient), nil
	}
	return NewKafkaProducer(topic, producer, c.logger), nil
}

// CreateTLSConfig return tls config
func CreateTLSConfig(tlsConfig auth.TLS) (*tls.Config, error) {
	if !tlsConfig.Enabled {
		return nil, nil
	}

	if tlsConfig.CertData != "" && tlsConfig.CertFile != "" {
		return nil, errors.New("Cannot specify both certData and certFile properties")
	}

	if tlsConfig.KeyData != "" && tlsConfig.KeyFile != "" {
		return nil, errors.New("Cannot specify both keyData and keyFile properties")
	}

	if tlsConfig.CaData != "" && tlsConfig.CaFile != "" {
		return nil, errors.New("Cannot specify both caData and caFile properties")
	}

	var certBytes []byte
	var keyBytes []byte
	var err error
	var cert tls.Certificate

	if tlsConfig.CertFile != "" {
		certBytes, err = ioutil.ReadFile(tlsConfig.CertFile)
		if err != nil {
			return nil, fmt.Errorf("error reading client certificate file: %w", err)
		}
	} else if tlsConfig.CertData != "" {
		certBytes, err = base64.StdEncoding.DecodeString(tlsConfig.CertData)
		if err != nil {
			return nil, fmt.Errorf("client certificate could not be decoded: %w", err)
		}
	}

	if tlsConfig.KeyFile != "" {
		keyBytes, err = ioutil.ReadFile(tlsConfig.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("error reading client certificate private key file: %w", err)
		}
	} else if tlsConfig.KeyData != "" {
		keyBytes, err = base64.StdEncoding.DecodeString(tlsConfig.KeyData)
		if err != nil {
			return nil, fmt.Errorf("client certificate private key could not be decoded: %w", err)
		}
	}

	if len(certBytes) > 0 {
		cert, err = tls.X509KeyPair(certBytes, keyBytes)
		if err != nil {
			return nil, fmt.Errorf("unable to generate x509 key pair: %w", err)
		}
	}

	caCertPool := x509.NewCertPool()

	if tlsConfig.CaFile != "" {
		pemData, err := ioutil.ReadFile(tlsConfig.CaFile)
		if err != nil {
			return nil, err
		}
		caCertPool.AppendCertsFromPEM(pemData)
	} else if tlsConfig.CaData != "" {
		pemData, err := base64.StdEncoding.DecodeString(tlsConfig.CaData)
		if err != nil {
			return nil, fmt.Errorf("caData could not be decoded: %w", err)
		}
		caCertPool.AppendCertsFromPEM(pemData)
	}

	return auth.NewDynamicTLSClientConfig(
		func() (*tls.Certificate, error) {
			return &cert, nil
		},
		caCertPool,
		"",
		tlsConfig.EnableHostVerification), nil
}
