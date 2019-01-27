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

package indexer

import (
	"fmt"
	"github.com/olivere/elastic"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/indexer"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
	es "github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"sync"
	"sync/atomic"
	"time"
)

type indexProcessor struct {
	appName         string
	consumerName    string
	kafkaClient     messaging.Client
	consumer        messaging.Consumer
	esClient        es.Client
	esProcessor     ESProcessor
	esProcessorName string
	esIndexName     string
	config          *Config
	logger          bark.Logger
	metricsClient   metrics.Client
	isStarted       int32
	isStopped       int32
	shutdownWG      sync.WaitGroup
	shutdownCh      chan struct{}
	msgEncoder      codec.BinaryEncoder
}

const (
	esDocIDDelimiter = "~"
	esDocType        = "_doc"

	versionTypeExternal = "external"
)

var (
	errUnknownMessageType = &shared.BadRequestError{Message: "unknown message type"}
)

func newIndexProcessor(appName, consumerName string, kafkaClient messaging.Client, esClient es.Client,
	esProcessorName, esIndexName string, config *Config, logger bark.Logger, metricsClient metrics.Client) *indexProcessor {
	return &indexProcessor{
		appName:         appName,
		consumerName:    consumerName,
		kafkaClient:     kafkaClient,
		esClient:        esClient,
		esProcessorName: esProcessorName,
		esIndexName:     esIndexName,
		config:          config,
		logger: logger.WithFields(bark.Fields{
			logging.TagWorkflowComponent: logging.TagValueIndexerProcessorComponent,
		}),
		metricsClient: metricsClient,
		shutdownCh:    make(chan struct{}),
		msgEncoder:    codec.NewThriftRWEncoder(),
	}
}

func (p *indexProcessor) Start() error {
	if !atomic.CompareAndSwapInt32(&p.isStarted, 0, 1) {
		return nil
	}

	logging.LogIndexProcessorStartingEvent(p.logger)
	consumer, err := p.kafkaClient.NewConsumer(p.appName, p.consumerName, p.config.IndexerConcurrency())
	if err != nil {
		logging.LogIndexProcessorStartFailedEvent(p.logger, err)
		return err
	}

	if err := consumer.Start(); err != nil {
		logging.LogIndexProcessorStartFailedEvent(p.logger, err)
		return err
	}

	esProcessor, err := NewESProcessorAndStart(p.config, p.esClient, p.esProcessorName, p.logger, p.metricsClient)
	if err != nil {
		logging.LogIndexProcessorStartFailedEvent(p.logger, err)
		return err
	}

	p.consumer = consumer
	p.esProcessor = esProcessor
	p.shutdownWG.Add(1)
	go p.processorPump()

	logging.LogIndexProcessorStartedEvent(p.logger)
	return nil
}

func (p *indexProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(&p.isStopped, 0, 1) {
		return
	}

	logging.LogIndexProcessorShuttingDownEvent(p.logger)
	defer logging.LogIndexProcessorShutDownEvent(p.logger)

	if atomic.LoadInt32(&p.isStarted) == 1 {
		close(p.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		logging.LogIndexProcessorShutDownTimedoutEvent(p.logger)
	}
}

func (p *indexProcessor) processorPump() {
	defer p.shutdownWG.Done()

	var workerWG sync.WaitGroup
	for workerID := 0; workerID < p.config.IndexerConcurrency(); workerID++ {
		workerWG.Add(1)
		go p.messageProcessLoop(&workerWG, workerID)
	}

	select {
	case <-p.shutdownCh:
		// Processor is shutting down, close the underlying consumer and esProcessor
		p.consumer.Stop()
		p.esProcessor.Stop()
	}

	p.logger.Info("Index processor pump shutting down.")
	if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
		p.logger.Warn("Index processor timed out on worker shutdown.")
	}
}

func (p *indexProcessor) messageProcessLoop(workerWG *sync.WaitGroup, workerID int) {
	defer workerWG.Done()

	for {
		select {
		case msg, ok := <-p.consumer.Messages():
			if !ok {
				p.logger.Info("Worker for index processor shutting down.")
				return // channel closed
			}
			err := p.process(msg)
			if err != nil {
				msg.Nack()
			}
		}
	}
}

func (p *indexProcessor) process(kafkaMsg messaging.Message) error {
	logger := p.logger.WithFields(bark.Fields{
		logging.TagPartitionKey: kafkaMsg.Partition(),
		logging.TagOffset:       kafkaMsg.Offset(),
		logging.TagAttemptStart: time.Now(),
	})

	indexMsg, err := p.deserialize(kafkaMsg.Value())
	if err != nil {
		logger.WithFields(bark.Fields{
			logging.TagErr: err,
		}).Error("Failed to deserialize index messages.")
		p.metricsClient.IncCounter(metrics.IndexProcessorScope, metrics.IndexProcessorCorruptedData)
		return err
	}

	return p.addMessageToES(indexMsg, kafkaMsg, logger)
}

func (p *indexProcessor) deserialize(payload []byte) (*indexer.Message, error) {
	var msg indexer.Message
	if err := p.msgEncoder.Decode(payload, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (p *indexProcessor) addMessageToES(indexMsg *indexer.Message, kafkaMsg messaging.Message, logger bark.Logger) error {
	docID := indexMsg.GetWorkflowID() + esDocIDDelimiter + indexMsg.GetRunID()

	var keyToKafkaMsg string
	var req elastic.BulkableRequest
	switch indexMsg.GetMessageType() {
	case indexer.MessageTypeIndex:
		keyToKafkaMsg = fmt.Sprintf("%v-%v", kafkaMsg.Partition(), kafkaMsg.Offset())
		doc := p.generateESDoc(indexMsg, keyToKafkaMsg)
		req = elastic.NewBulkIndexRequest().
			Index(p.esIndexName).
			Type(esDocType).
			Id(docID).
			VersionType(versionTypeExternal).
			Version(indexMsg.GetVersion()).
			Doc(doc)
	case indexer.MessageTypeDelete:
		keyToKafkaMsg = docID
		req = elastic.NewBulkDeleteRequest().
			Index(p.esIndexName).
			Type(esDocType).
			Id(docID).
			VersionType(versionTypeExternal).
			Version(indexMsg.GetVersion())
	default:
		logger.Error("Unknown message type")
		p.metricsClient.IncCounter(metrics.IndexProcessorScope, metrics.IndexProcessorCorruptedData)
		return errUnknownMessageType
	}

	p.esProcessor.Add(req, keyToKafkaMsg, kafkaMsg)
	return nil
}

func (p *indexProcessor) generateESDoc(msg *indexer.Message, keyToKafkaMsg string) map[string]interface{} {
	doc := p.dumpFieldsToMap(msg.IndexAttributes.Fields)
	fulfillDoc(doc, msg, keyToKafkaMsg)
	return doc
}

func (p *indexProcessor) dumpFieldsToMap(fields map[string]*indexer.Field) map[string]interface{} {
	doc := make(map[string]interface{})
	for k, v := range fields {
		if !es.IsFieldNameValid(k) {
			p.logger.WithFields(bark.Fields{
				logging.TagESField: k,
			}).Error("Unregistered field.")
			p.metricsClient.IncCounter(metrics.IndexProcessorScope, metrics.IndexProcessorCorruptedData)
			continue
		}

		switch v.GetType() {
		case indexer.FieldTypeString:
			doc[k] = v.GetStringData()
		case indexer.FieldTypeInt:
			doc[k] = v.GetIntData()
		case indexer.FieldTypeBool:
			doc[k] = v.GetBoolData()
		default:
			// must be bug in code and bad deployment, check data sent from producer
			p.logger.Fatalf("Unknown field type")
		}
	}
	return doc
}

func fulfillDoc(doc map[string]interface{}, msg *indexer.Message, keyToKafkaMsg string) {
	doc[es.DomainID] = msg.GetDomainID()
	doc[es.WorkflowID] = msg.GetWorkflowID()
	doc[es.RunID] = msg.GetRunID()
	doc[es.KafkaKey] = keyToKafkaMsg
}
