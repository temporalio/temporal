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
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/olivere/elastic/v7"
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
		Add(request *elasticsearch.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool)
	}

	// esProcessorImpl implements Processor, it's an agent of elastic.BulkProcessor
	esProcessorImpl struct {
		status                  int32
		bulkProcessor           elasticsearch.BulkProcessor
		bulkProcessorParameters *elasticsearch.BulkProcessorParameters
		client                  elasticsearch.Client
		mapToAckChan            collection.ConcurrentTxMap // used to map ES request to ack channel
		logger                  log.Logger
		metricsClient           metrics.Client
		indexerConcurrency      uint32
	}

	ackChanWithStopwatch struct { // value of esProcessorImpl.mapToAckChan
		ackCh                 chan<- bool
		addToProcessStopwatch *tally.Stopwatch // Used to report metrics: interval between visibility task being added to bulk processor and it is processed (ack/nack).
	}
)

var _ Processor = (*esProcessorImpl)(nil)

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
func (p *esProcessorImpl) Add(request *elasticsearch.BulkableRequest, visibilityTaskKey string, ackCh chan<- bool) {
	if cap(ackCh) < 1 {
		panic("ackCh must be buffered channel (length should be 1 or more)")
	}

	sw := p.metricsClient.StartTimer(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorRequestLatency)
	ackChWithStopwatch := newAckChanWithStopwatch(ackCh, &sw)
	_, isDup, _ := p.mapToAckChan.PutOrDo(visibilityTaskKey, ackChWithStopwatch, func(key interface{}, value interface{}) error {
		ackChWithStopwatchExisting, ok := value.(*ackChanWithStopwatch)
		if !ok {
			p.logger.Fatal(fmt.Sprintf("mapToAckChan has item of a wrong type %T (%T expected).", value, &ackChanWithStopwatch{}), tag.Value(key))
		}

		p.logger.Warn("Adding duplicate ES request for visibility task key.", tag.Key(visibilityTaskKey), tag.ESDocID(request.ID), tag.Value(request.Doc))

		// Nack existing visibility task.
		ackChWithStopwatchExisting.addToProcessStopwatch.Stop()
		ackChWithStopwatchExisting.ackCh <- false

		// Replace existing dictionary item with new item.
		// Note: request won't be added to bulk processor.
		ackChWithStopwatchExisting.addToProcessStopwatch = ackChWithStopwatch.addToProcessStopwatch
		ackChWithStopwatchExisting.ackCh = ackChWithStopwatch.ackCh
		return nil
	})
	if isDup {
		return
	}
	p.bulkProcessor.Add(request)
}

// bulkBeforeAction is triggered before bulk processor commit
func (p *esProcessorImpl) bulkBeforeAction(_ int64, requests []elastic.BulkableRequest) {
	p.metricsClient.AddCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorRequests, int64(len(requests)))
}

// bulkAfterAction is triggered after bulk processor commit
func (p *esProcessorImpl) bulkAfterAction(_ int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		// This happens after configured retry, which means something bad happens on cluster or index
		// When cluster back to live, processor will re-commit those failure requests

		isRetryable := isRetryableError(err)
		p.logger.Error("Error commit bulk ES request.", tag.Error(err), tag.Bool(isRetryable))
		for _, request := range requests {
			p.logger.Error("ES request failed.", tag.ESRequest(request.String()))
			p.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorFailures)

			if !isRetryable {
				visibilityTaskKey := p.getVisibilityTaskKey(request)
				if visibilityTaskKey == "" {
					continue
				}
				p.sendToAckChan(visibilityTaskKey, false)
			}
		}
		return
	}

	for i, request := range requests {
		visibilityTaskKey := p.getVisibilityTaskKey(request)
		if visibilityTaskKey == "" {
			continue
		}
		if i >= len(response.Items) {
			p.logger.Error("ES request failed. Request item doesn't have corresponding response item.",
				tag.Value(i),
				tag.Key(visibilityTaskKey),
				tag.ESRequest(request.String()))
			p.sendToAckChan(visibilityTaskKey, false)
			continue
		}

		responseItem := response.Items[i]
		for _, resp := range responseItem {
			switch {
			case isSuccessStatus(resp.Status):
				p.sendToAckChan(visibilityTaskKey, true)
			case !isRetryableStatus(resp.Status):
				p.logger.Error("ES request failed.",
					tag.ESResponseStatus(resp.Status),
					tag.ESResponseError(getErrorMsgFromESResp(resp)),
					tag.ESRequest(request.String()))
				p.sendToAckChan(visibilityTaskKey, false)
			default: // bulk processor will retry
				p.logger.Warn("ES request retried.",
					tag.ESResponseStatus(resp.Status),
					tag.ESResponseError(getErrorMsgFromESResp(resp)),
					tag.ESRequest(request.String()))
				p.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorRetries)
			}
		}
	}
}

func (p *esProcessorImpl) sendToAckChan(visibilityTaskKey string, ack bool) {
	// Use RemoveIf here to prevent race condition with de-dup logic in Add method.
	_ = p.mapToAckChan.RemoveIf(visibilityTaskKey, func(key interface{}, value interface{}) bool {
		ackChWithStopwatch, ok := value.(*ackChanWithStopwatch)
		if !ok {
			p.logger.Fatal(fmt.Sprintf("mapToAckChan has item of a wrong type %T (%T expected).", value, &ackChanWithStopwatch{}), tag.ESKey(visibilityTaskKey))
		}

		ackChWithStopwatch.addToProcessStopwatch.Stop()
		ackChWithStopwatch.ackCh <- ack
		return true
	})
}

func (p *esProcessorImpl) getVisibilityTaskKey(request elastic.BulkableRequest) string {
	req, err := request.Source()
	if err != nil {
		p.logger.Error("Get request source err.", tag.Error(err), tag.ESRequest(request.String()))
		p.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorCorruptedData)
		return ""
	}

	var key string
	if len(req) == 2 { // index or update requests
		var body map[string]interface{}
		if err := json.Unmarshal([]byte(req[1]), &body); err != nil {
			p.logger.Error("Unmarshal index request body err.", tag.Error(err))
			p.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorCorruptedData)
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
			p.metricsClient.IncCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorCorruptedData)
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
func isSuccessStatus(status int) bool {
	if status >= 200 && status < 300 || status == 409 || status == 404 {
		return true
	}
	return false
}

// isRetryableStatus is complaint with elastic.BulkProcessorService.RetryItemStatusCodes
// responses with these status will be kept in queue and retried until success
// 408 - Request Timeout
// 429 - Too Many Requests
// 500 - Node not connected
// 503 - Service Unavailable
// 507 - Insufficient Storage
func isRetryableStatus(status int) bool {
	switch status {
	case 408, 429, 500, 503, 507:
		return true
	}
	return false
}

func isRetryableError(err error) bool {
	switch e := err.(type) {
	case *elastic.Error:
		return isRetryableStatus(e.Status)
	default:
		return false
	}
}

func getErrorMsgFromESResp(resp *elastic.BulkResponseItem) string {
	if resp.Error != nil {
		return resp.Error.Reason
	}
	return ""
}

func newAckChanWithStopwatch(ackCh chan<- bool, stopwatch *tally.Stopwatch) *ackChanWithStopwatch {
	return &ackChanWithStopwatch{
		ackCh:                 ackCh,
		addToProcessStopwatch: stopwatch,
	}
}
