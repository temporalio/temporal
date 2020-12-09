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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination esProcessor_mock.go
package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/olivere/elastic"
	"github.com/uber-go/tally"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/elasticsearch"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	// Processor is interface for elastic search bulk processor
	Processor interface {
		common.Daemon

		// Add request to bulk, and record kafka message in map with provided key
		// This call will be blocked when downstream has issues
		Add(request elastic.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool)
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

	// esProcessorImpl implements Processor, it's an agent of elastic.BulkProcessor
	esProcessorImpl struct {
		status                  int32
		bulkProcessor           ElasticBulkProcessor
		bulkProcessorParameters *elasticsearch.BulkProcessorParameters
		client                  elasticsearch.Client
		mapToAckChan            collection.ConcurrentTxMap // used to map ES request to ack channel
		logger                  log.Logger
		metricsClient           metrics.Client
		indexerConcurrency      uint32
	}

	chanWithStopwatch struct { // value of esProcessorImpl.mapToAckChan
		ackCh             chan<- bool
		addToAckStopwatch *tally.Stopwatch // metric from message add to process, to message ack/nack
	}
)

var _ Processor = (*esProcessorImpl)(nil)
var _ ElasticBulkProcessor = (*elastic.BulkProcessor)(nil)

const (
	// retry configs for es bulk processor
	esProcessorInitialRetryInterval = 200 * time.Millisecond
	esProcessorMaxRetryInterval     = 20 * time.Second
	visibilityProcessorName         = "visibility-processor"
)

// NewProcessor create new esProcessorImpl
func NewProcessor(
	cfg *ProcessorConfig,
	client elasticsearch.Client,
	logger log.Logger,
	metricsClient metrics.Client,
) *esProcessorImpl {

	p := &esProcessorImpl{
		status:             common.DaemonStatusInitialized,
		client:             client,
		logger:             logger.WithTags(tag.ComponentIndexerESProcessor),
		metricsClient:      metricsClient,
		indexerConcurrency: uint32(cfg.IndexerConcurrency()),
		bulkProcessorParameters: &elasticsearch.BulkProcessorParameters{
			Name:          visibilityProcessorName,
			NumOfWorkers:  cfg.ESProcessorNumOfWorkers(),
			BulkActions:   cfg.ESProcessorBulkActions(),
			BulkSize:      cfg.ESProcessorBulkSize(),
			FlushInterval: cfg.ESProcessorFlushInterval(),
			Backoff:       elastic.NewExponentialBackoff(esProcessorInitialRetryInterval, esProcessorMaxRetryInterval),
		},
	}
	p.bulkProcessorParameters.AfterFunc = p.bulkAfterAction
	p.bulkProcessorParameters.BeforeFunc = p.bulkBeforeAction
	return p
}

func (p *esProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&p.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	var err error
	p.mapToAckChan = collection.NewShardedConcurrentTxMap(1024, p.hashFn)
	p.bulkProcessor, err = p.client.RunBulkProcessor(context.Background(), p.bulkProcessorParameters)
	if err != nil {
		p.logger.Fatal("Unable to start Elastic Search processor.", tag.LifeCycleStartFailed, tag.Error(err))
	}
}

func (p *esProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&p.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	err := p.bulkProcessor.Stop()
	if err != nil {
		p.logger.Fatal("Unable to stop Elastic Search processor.", tag.LifeCycleStopFailed, tag.Error(err))
	}
	p.mapToAckChan = nil
	p.bulkProcessor = nil
}

func (p *esProcessorImpl) hashFn(key interface{}) uint32 {
	id, ok := key.(string)
	if !ok {
		return 0
	}
	idBytes := []byte(id)
	hash := farm.Hash32(idBytes)
	return hash % p.indexerConcurrency
}

// Add request to bulk, and record ack channel message in map with provided key
func (p *esProcessorImpl) Add(request elastic.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
	if cap(ackCh) < 1 {
		panic("ackCh must be buffered channel (length should be 1 or more)")
	}

	actionWhenFoundDuplicates := func(key interface{}, value interface{}) error {
		ackCh <- true
		return nil
	}

	sw := p.metricsClient.StartTimer(metrics.ESProcessorScope, metrics.ESProcessorProcessMsgLatency)
	chWithStopwatch := newChanWithStopwatch(ackCh, &sw)
	_, isDup, _ := p.mapToAckChan.PutOrDo(visibilityTaskKey, chWithStopwatch, actionWhenFoundDuplicates)
	if isDup {
		return
	}
	p.bulkProcessor.Add(request)
}

// bulkBeforeAction is triggered before bulk processor commit
func (p *esProcessorImpl) bulkBeforeAction(_ int64, requests []elastic.BulkableRequest) {
	p.metricsClient.AddCounter(metrics.ESProcessorScope, metrics.ESProcessorRequests, int64(len(requests)))
}

// bulkAfterAction is triggered after bulk processor commit
func (p *esProcessorImpl) bulkAfterAction(_ int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
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
		visibilityTaskKey := p.getVisibilityTaskKey(requests[i])
		if visibilityTaskKey == "" {
			continue
		}
		responseItem := responseItems[i]
		for _, resp := range responseItem {
			switch {
			case isResponseSuccess(resp.Status):
				p.sendToAckChan(visibilityTaskKey, true)
			case !isResponseRetryable(resp.Status):
				wid, rid, namespaceID := p.getDocIDs(requests[i])
				p.logger.Error("ES request failed.",
					tag.ESResponseStatus(resp.Status), tag.ESResponseError(getErrorMsgFromESResp(resp)), tag.WorkflowID(wid), tag.WorkflowRunID(rid),
					tag.WorkflowNamespaceID(namespaceID))
				p.sendToAckChan(visibilityTaskKey, false)
			default: // bulk processor will retry
				p.logger.Info("ES request retried.", tag.ESResponseStatus(resp.Status))
				p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorRetries)
			}
		}
	}
}

func (p *esProcessorImpl) sendToAckChan(visibilityTaskKey string, ack bool) {
	ackCh, ok := p.getAckChan(visibilityTaskKey)
	if !ok {
		return
	}
	ackCh <- ack

	p.mapToAckChan.Remove(visibilityTaskKey)
}

func (p *esProcessorImpl) getAckChan(visibilityTaskKey string) (chan<- bool, bool) {
	msg, ok := p.mapToAckChan.Get(visibilityTaskKey)
	if !ok {
		return nil, false
	}
	chWithStopwatch, ok := msg.(*chanWithStopwatch)
	if !ok { // must be bug in code and bad deployment
		p.logger.Fatal(fmt.Sprintf("Message has wrong type %T (%T expected).", msg, &chanWithStopwatch{}), tag.ESKey(visibilityTaskKey))
	}
	return chWithStopwatch.ackCh, ok
}

func (p *esProcessorImpl) getVisibilityTaskKey(request elastic.BulkableRequest) string {
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

		k, ok := body[definition.VisibilityTaskKey]
		if !ok {
			// must be bug in code and bad deployment, check processor that add es requests
			panic("VisibilityTaskKey not found")
		}
		key, ok = k.(string)
		if !ok {
			// must be bug in code and bad deployment, check processor that add es requests
			panic("VisibilityTaskKey is not string")
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

func (p *esProcessorImpl) getDocIDs(request elastic.BulkableRequest) (workflowID string, runID string, namespaceID string) {
	// TODO (alex): This need to be combined with getVisibilityTaskKey
	req, err := request.Source()
	if err != nil {
		p.logger.Error("Get request source err.", tag.Error(err), tag.ESRequest(request.String()))
		p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorCorruptedData)
		return
	}

	if len(req) == 2 { // index or update requests
		var body map[string]interface{}
		if err := json.Unmarshal([]byte(req[1]), &body); err != nil {
			p.logger.Error("Unmarshal index request body err.", tag.Error(err))
			p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorCorruptedData)
			return
		}

		wID, _ := body[definition.WorkflowID]
		workflowID, _ = wID.(string)

		rID, _ := body[definition.RunID]
		runID, _ = rID.(string)

		nID, _ := body[definition.NamespaceID]
		namespaceID, _ = nID.(string)
	} else { // delete requests
		var body map[string]map[string]interface{}
		if err := json.Unmarshal([]byte(req[0]), &body); err != nil {
			p.logger.Error("Unmarshal delete request body err.", tag.Error(err))
			p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorCorruptedData)
			return
		}

		opMap, ok := body["delete"]
		if !ok {
			return
		}
		id, _ := opMap["_id"]
		docID, _ := id.(string)
		wrIDs := strings.Split(docID, delimiter)
		if len(wrIDs) > 0 {
			workflowID = wrIDs[0]
		}
		if len(wrIDs) > 1 {
			runID = wrIDs[1]
		}
	}
	return
}

// 409 - Version Conflict
// 404 - Not Found
func isResponseSuccess(status int) bool {
	if status >= 200 && status < 300 || status == 409 || status == 404 {
		return true
	}
	return false
}

// isResponseRetryable is complaint with elastic.BulkProcessorService.RetryItemStatusCodes
// responses with these status will be kept in queue and retried until success
// 408 - Request Timeout
// 429 - Too Many Requests
// 500 - Node not connected
// 503 - Service Unavailable
// 507 - Insufficient Storage
func isResponseRetryable(status int) bool {
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

func newChanWithStopwatch(ackCh chan<- bool, stopwatch *tally.Stopwatch) *chanWithStopwatch {
	return &chanWithStopwatch{
		ackCh:             ackCh,
		addToAckStopwatch: stopwatch,
	}
}

func (km *chanWithStopwatch) Ack() {
	km.ackCh <- true
	if km.addToAckStopwatch != nil {
		km.addToAckStopwatch.Stop()
	}
}

func (km *chanWithStopwatch) Nack() {
	km.ackCh <- false
	if km.addToAckStopwatch != nil {
		km.addToAckStopwatch.Stop()
	}
}
