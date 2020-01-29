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
	"context"
	"encoding/json"
	"time"

	"github.com/olivere/elastic"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/collection"
	es "github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
)

type (
	// ESProcessor is interface for elastic search bulk processor
	ESProcessor interface {
		// Stop processor and clean up
		Stop()
		// Add request to bulk, and record kafka message in map with provided key
		// This call will be blocked when downstream has issues
		Add(request elastic.BulkableRequest, key string, kafkaMsg messaging.Message)
	}

	// ElasticBulkProcessor is interface for elastic.BulkProcessor
	// (elastic package doesn't provide such interface that tests can mock)
	ElasticBulkProcessor interface {
		Start(ctx context.Context) error
		Stop() error
		Close() error
		Stats() elastic.BulkProcessorStats
		Add(request elastic.BulkableRequest)
		Flush() error
	}

	// esProcessorImpl implements ESProcessor, it's an agent of elastic.BulkProcessor
	esProcessorImpl struct {
		processor     ElasticBulkProcessor
		mapToKafkaMsg collection.ConcurrentTxMap // used to map ES request to kafka message
		config        *Config
		logger        log.Logger
		metricsClient metrics.Client
	}

	kafkaMessageWithMetrics struct { // value of esProcessorImpl.mapToKafkaMsg
		message        messaging.Message
		swFromAddToAck *tally.Stopwatch // metric from message add to process, to message ack/nack
	}
)

var _ ESProcessor = (*esProcessorImpl)(nil)
var _ ElasticBulkProcessor = (*elastic.BulkProcessor)(nil)

const (
	// retry configs for es bulk processor
	esProcessorInitialRetryInterval = 200 * time.Millisecond
	esProcessorMaxRetryInterval     = 20 * time.Second
)

// NewESProcessorAndStart create new ESProcessor and start
func NewESProcessorAndStart(config *Config, client es.Client, processorName string,
	logger log.Logger, metricsClient metrics.Client) (ESProcessor, error) {
	p := &esProcessorImpl{
		config:        config,
		logger:        logger.WithTags(tag.ComponentIndexerESProcessor),
		metricsClient: metricsClient,
	}

	params := &es.BulkProcessorParameters{
		Name:          processorName,
		NumOfWorkers:  config.ESProcessorNumOfWorkers(),
		BulkActions:   config.ESProcessorBulkActions(),
		BulkSize:      config.ESProcessorBulkSize(),
		FlushInterval: config.ESProcessorFlushInterval(),
		Backoff:       elastic.NewExponentialBackoff(esProcessorInitialRetryInterval, esProcessorMaxRetryInterval),
		BeforeFunc:    p.bulkBeforeAction,
		AfterFunc:     p.bulkAfterAction,
	}
	processor, err := client.RunBulkProcessor(context.Background(), params)
	if err != nil {
		return nil, err
	}

	p.processor = processor
	p.mapToKafkaMsg = collection.NewShardedConcurrentTxMap(1024, p.hashFn)
	return p, nil
}

func (p *esProcessorImpl) Stop() {
	p.processor.Stop() //nolint:errcheck
	p.mapToKafkaMsg = nil
}

// Add an ES request, and an map item for kafka message
func (p *esProcessorImpl) Add(request elastic.BulkableRequest, key string, kafkaMsg messaging.Message) {
	actionWhenFoundDuplicates := func(key interface{}, value interface{}) error {
		return kafkaMsg.Ack()
	}
	sw := p.metricsClient.StartTimer(metrics.ESProcessorScope, metrics.ESProcessorProcessMsgLatency)
	mapVal := newKafkaMessageWithMetrics(kafkaMsg, &sw)
	_, isDup, _ := p.mapToKafkaMsg.PutOrDo(key, mapVal, actionWhenFoundDuplicates)
	if isDup {
		return
	}
	p.processor.Add(request)
}

// bulkBeforeAction is triggered before bulk processor commit
func (p *esProcessorImpl) bulkBeforeAction(executionID int64, requests []elastic.BulkableRequest) {
	p.metricsClient.AddCounter(metrics.ESProcessorScope, metrics.ESProcessorRequests, int64(len(requests)))
}

// bulkAfterAction is triggered after bulk processor commit
func (p *esProcessorImpl) bulkAfterAction(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		// This happens after configured retry, which means something bad happens on cluster or index
		// When cluster back to live, processor will re-commit those failure requests
		p.logger.Error("Error commit bulk request.", tag.Error(err))

		for _, request := range requests {
			p.logger.Error("ES request failed.", tag.ESRequest(request.String()))
			p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorFailures)
		}
		return
	}

	responseItems := response.Items
	for i := 0; i < len(requests); i++ {
		key := p.getKeyForKafkaMsg(requests[i])
		if key == "" {
			continue
		}
		responseItem := responseItems[i]
		for _, resp := range responseItem {
			switch {
			case isResponseSuccess(resp.Status):
				p.ackKafkaMsg(key)
			case !isResponseRetriable(resp.Status):
				p.logger.Error("ES request failed.",
					tag.ESResponseStatus(resp.Status), tag.ESResponseError(getErrorMsgFromESResp(resp)))
				p.nackKafkaMsg(key)
			default: // bulk processor will retry
				p.logger.Info("ES request retried.", tag.ESResponseStatus(resp.Status))
				p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorRetries)
			}
		}
	}
}

func (p *esProcessorImpl) ackKafkaMsg(key string) {
	p.ackKafkaMsgHelper(key, false)
}

func (p *esProcessorImpl) nackKafkaMsg(key string) {
	p.ackKafkaMsgHelper(key, true)
}

func (p *esProcessorImpl) ackKafkaMsgHelper(key string, nack bool) {
	msg, ok := p.mapToKafkaMsg.Get(key)
	if !ok {
		return // duplicate kafka message
	}
	kafkaMsg, ok := msg.(*kafkaMessageWithMetrics)
	if !ok { // must be bug in code and bad deployment
		p.logger.Fatal("Message is not kafka message.", tag.ESKey(key))
	}

	if nack {
		kafkaMsg.Nack()
	} else {
		kafkaMsg.Ack()
	}

	p.mapToKafkaMsg.Remove(key)
}

func (p *esProcessorImpl) hashFn(key interface{}) uint32 {
	id, ok := key.(string)
	if !ok {
		return 0
	}
	numOfShards := p.config.IndexerConcurrency()
	return uint32(common.WorkflowIDToHistoryShard(id, numOfShards))
}

func (p *esProcessorImpl) getKeyForKafkaMsg(request elastic.BulkableRequest) string {
	req, err := request.Source()
	if err != nil {
		p.logger.Error("Get request source err.", tag.Error(err), tag.ESRequest(request.String()))
		p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorCorruptedData)
		return ""
	}

	var key string
	if len(req) == 2 { // index or update requests
		var body map[string]interface{}
		if err := json.Unmarshal([]byte(req[1]), &body); err != nil {
			p.logger.Error("Unmarshal index request body err.", tag.Error(err))
			p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorCorruptedData)
			return ""
		}

		k, ok := body[es.KafkaKey]
		if !ok {
			// must be bug in code and bad deployment, check processor that add es requests
			panic("KafkaKey not found")
		}
		key, ok = k.(string)
		if !ok {
			// must be bug in code and bad deployment, check processor that add es requests
			panic("KafkaKey is not string")
		}
	} else { // delete requests
		var body map[string]map[string]interface{}
		if err := json.Unmarshal([]byte(req[0]), &body); err != nil {
			p.logger.Error("Unmarshal delete request body err.", tag.Error(err))
			p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorCorruptedData)
			return ""
		}

		opMap, ok := body["delete"]
		if !ok {
			// must be bug, check if dependency changed
			panic("delete key not found in request")
		}
		k, ok := opMap["_id"]
		if !ok {
			// must be bug in code and bad deployment, check processor that add es requests
			panic("_id not found in request opMap")
		}
		key, _ = k.(string)
	}
	return key
}

// 409 - Version Conflict
// 404 - Not Found
func isResponseSuccess(status int) bool {
	if status >= 200 && status < 300 || status == 409 || status == 404 {
		return true
	}
	return false
}

// isResponseRetriable is complaint with elastic.BulkProcessorService.RetryItemStatusCodes
// responses with these status will be kept in queue and retried until success
// 408 - Request Timeout
// 429 - Too Many Requests
// 500 - Node not connected
// 503 - Service Unavailable
// 507 - Insufficient Storage
func isResponseRetriable(status int) bool {
	switch status {
	case 408, 429, 500, 503, 507:
		return true
	}
	return false
}

func getErrorMsgFromESResp(resp *elastic.BulkResponseItem) string {
	var errMsg string
	if resp.Error != nil {
		errMsg = resp.Error.Reason
	}
	return errMsg
}

func newKafkaMessageWithMetrics(kafkaMsg messaging.Message, stopwatch *tally.Stopwatch) *kafkaMessageWithMetrics {
	return &kafkaMessageWithMetrics{
		message:        kafkaMsg,
		swFromAddToAck: stopwatch,
	}
}

func (km *kafkaMessageWithMetrics) Ack() {
	km.message.Ack() //nolint:errcheck
	if km.swFromAddToAck != nil {
		km.swFromAddToAck.Stop()
	}
}

func (km *kafkaMessageWithMetrics) Nack() {
	km.message.Nack() //nolint:errcheck
	if km.swFromAddToAck != nil {
		km.swFromAddToAck.Stop()
	}
}
