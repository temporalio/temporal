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

//go:generate mockgen -copyright_file ../../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination processor_mock.go

package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/olivere/elastic/v7"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// Processor is interface for elastic search bulk processor
	Processor interface {
		common.Daemon

		// Add request to bulk processor.
		Add(request *client.BulkableRequest, visibilityTaskKey string) <-chan bool
	}

	// processorImpl implements Processor, it's an agent of elastic.BulkProcessor
	processorImpl struct {
		status                  int32
		bulkProcessor           client.BulkProcessor
		bulkProcessorParameters *client.BulkProcessorParameters
		client                  client.Client
		mapToAckChan            collection.ConcurrentTxMap // used to map ES request to ack channel
		logger                  log.Logger
		metricsClient           metrics.Client
		indexerConcurrency      uint32
	}

	// ProcessorConfig contains all configs for processor
	ProcessorConfig struct {
		IndexerConcurrency dynamicconfig.IntPropertyFn
		// TODO: remove ESProcessor prefix
		ESProcessorNumOfWorkers  dynamicconfig.IntPropertyFn
		ESProcessorBulkActions   dynamicconfig.IntPropertyFn // max number of requests in bulk
		ESProcessorBulkSize      dynamicconfig.IntPropertyFn // max total size of bytes in bulk
		ESProcessorFlushInterval dynamicconfig.DurationPropertyFn

		ESProcessorAckTimeout dynamicconfig.DurationPropertyFn
	}

	ackChan struct { // value of processorImpl.mapToAckChan
		ackChInternal chan bool
		createdAt     time.Time    // Time when request was created (used to report metrics).
		addedAt       atomic.Value // of time.Time // Time when request was added to bulk processor (used to report metrics).
		startedAt     time.Time    // Time when request was sent to Elasticsearch by bulk processor (used to report metrics).
	}
)

var _ Processor = (*processorImpl)(nil)

const (
	// retry configs for es bulk processor
	esProcessorInitialRetryInterval = 200 * time.Millisecond
	esProcessorMaxRetryInterval     = 20 * time.Second
	visibilityProcessorName         = "visibility-processor"
)

// NewProcessor create new processorImpl
func NewProcessor(
	cfg *ProcessorConfig,
	esClient client.Client,
	logger log.Logger,
	metricsClient metrics.Client,
) *processorImpl {

	p := &processorImpl{
		status:             common.DaemonStatusInitialized,
		client:             esClient,
		logger:             log.With(logger, tag.ComponentIndexerESProcessor),
		metricsClient:      metricsClient,
		indexerConcurrency: uint32(cfg.IndexerConcurrency()),
		bulkProcessorParameters: &client.BulkProcessorParameters{
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

func (p *processorImpl) Start() {
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
		p.logger.Fatal("Unable to start Elasticsearch processor.", tag.LifeCycleStartFailed, tag.Error(err))
	}
}

func (p *processorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&p.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	err := p.bulkProcessor.Stop()
	if err != nil {
		p.logger.Fatal("Unable to stop Elasticsearch processor.", tag.LifeCycleStopFailed, tag.Error(err))
	}
	p.mapToAckChan = nil
	p.bulkProcessor = nil
}

func (p *processorImpl) hashFn(key interface{}) uint32 {
	id, ok := key.(string)
	if !ok {
		return 0
	}
	idBytes := []byte(id)
	hash := farm.Hash32(idBytes)
	return hash % p.indexerConcurrency
}

// Add request to the bulk and return ack channel which will receive ack signal when request is processed.
func (p *processorImpl) Add(request *client.BulkableRequest, visibilityTaskKey string) <-chan bool {
	ackCh := newAckChan()
	_, isDup, _ := p.mapToAckChan.PutOrDo(visibilityTaskKey, ackCh, func(key interface{}, value interface{}) error {
		ackChExisting, ok := value.(*ackChan)
		if !ok {
			p.logger.Fatal(fmt.Sprintf("mapToAckChan has item of a wrong type %T (%T expected).", value, &ackChan{}), tag.Value(key))
		}

		p.logger.Warn("Skipping duplicate ES request for visibility task key.", tag.Key(visibilityTaskKey), tag.ESDocID(request.ID), tag.Value(request.Doc), tag.NewDurationTag("interval-between-duplicates", ackCh.createdAt.Sub(ackChExisting.createdAt)))
		p.metricsClient.IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorDuplicateRequest)

		// Ack duplicate visibility task right away as if it is processed successfully.
		ackCh.done(true, p.metricsClient)
		return nil
	})
	if !isDup {
		p.bulkProcessor.Add(request)
		ackCh.recordAdd(p.metricsClient)
	}
	return ackCh.ackChInternal
}

// bulkBeforeAction is triggered before bulk processor commit
func (p *processorImpl) bulkBeforeAction(_ int64, requests []elastic.BulkableRequest) {
	p.metricsClient.AddCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequests, int64(len(requests)))
	p.metricsClient.RecordDistribution(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorBulkSize, len(requests))
	p.metricsClient.RecordDistribution(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorQueuedRequests, p.mapToAckChan.Len()-len(requests))

	for _, request := range requests {
		visibilityTaskKey := p.extractVisibilityTaskKey(request)
		if visibilityTaskKey == "" {
			continue
		}
		_, _, _ = p.mapToAckChan.GetAndDo(visibilityTaskKey, func(key interface{}, value interface{}) error {
			ackCh, ok := value.(*ackChan)
			if !ok {
				p.logger.Fatal(fmt.Sprintf("mapToAckChan has item of a wrong type %T (%T expected).", value, &ackChan{}), tag.Value(key))
			}
			ackCh.recordStart(p.metricsClient)
			return nil
		})
	}
}

// bulkAfterAction is triggered after bulk processor commit
func (p *processorImpl) bulkAfterAction(_ int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		const logFirstNRequests = 5
		httpStatus := client.HttpStatus(err)
		isRetryable := client.IsRetryableStatus(httpStatus)
		var logRequests strings.Builder
		for i, request := range requests {
			if i < logFirstNRequests {
				logRequests.WriteString(request.String())
				logRequests.WriteRune('\n')
			}
			p.metricsClient.Scope(metrics.ElasticsearchBulkProcessor, metrics.HttpStatusTag(httpStatus)).IncCounter(metrics.ElasticsearchBulkProcessorFailures)

			if !isRetryable {
				visibilityTaskKey := p.extractVisibilityTaskKey(request)
				if visibilityTaskKey == "" {
					continue
				}
				p.sendToAckChan(visibilityTaskKey, false)
			}
		}
		p.logger.Error("Unable to commit bulk ES request.", tag.Error(err), tag.IsRetryable(isRetryable), tag.RequestCount(len(requests)), tag.ESRequest(logRequests.String()))
		return
	}

	responseIndex := p.buildResponseIndex(response)
	for i, request := range requests {
		visibilityTaskKey := p.extractVisibilityTaskKey(request)
		if visibilityTaskKey == "" {
			continue
		}

		docID := p.extractDocID(request)
		responseItem, ok := responseIndex[docID]
		if !ok {
			p.logger.Error("ES request failed. Request item doesn't have corresponding response item.",
				tag.Value(i),
				tag.Key(visibilityTaskKey),
				tag.ESDocID(docID),
				tag.ESRequest(request.String()))
			p.metricsClient.IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCorruptedData)
			p.sendToAckChan(visibilityTaskKey, false)
			continue
		}

		switch {
		case isSuccess(responseItem):
			p.sendToAckChan(visibilityTaskKey, true)
		case !client.IsRetryableStatus(responseItem.Status):
			p.logger.Error("ES request failed.",
				tag.ESResponseStatus(responseItem.Status),
				tag.ESResponseError(extractErrorReason(responseItem)),
				tag.Key(visibilityTaskKey),
				tag.ESDocID(docID),
				tag.ESRequest(request.String()))
			p.metricsClient.Scope(metrics.ElasticsearchBulkProcessor, metrics.HttpStatusTag(responseItem.Status)).IncCounter(metrics.ElasticsearchBulkProcessorFailures)
			p.sendToAckChan(visibilityTaskKey, false)
		default: // bulk processor will retry
			p.logger.Warn("ES request retried.",
				tag.ESResponseStatus(responseItem.Status),
				tag.ESResponseError(extractErrorReason(responseItem)),
				tag.Key(visibilityTaskKey),
				tag.ESDocID(docID),
				tag.ESRequest(request.String()))
			p.metricsClient.Scope(metrics.ElasticsearchBulkProcessor, metrics.HttpStatusTag(responseItem.Status)).IncCounter(metrics.ElasticsearchBulkProcessorRetries)
		}
	}
}

func (p *processorImpl) buildResponseIndex(response *elastic.BulkResponse) map[string]*elastic.BulkResponseItem {
	result := make(map[string]*elastic.BulkResponseItem)
	for _, operationResponseItemMap := range response.Items {
		for _, responseItem := range operationResponseItemMap {
			existingResponseItem, duplicateID := result[responseItem.Id]
			// In some rare cases there might be duplicate document Ids in the same bulk.
			// (for example, if two sequential upsert search attributes operation for the same workflow run end up being in the same bulk request)
			// In this case, item with greater status code (error) will overwrite existing item with smaller status code.
			if !duplicateID || existingResponseItem.Status < responseItem.Status {
				result[responseItem.Id] = responseItem
			}
		}
	}
	return result
}

func (p *processorImpl) sendToAckChan(visibilityTaskKey string, ack bool) {
	// Use RemoveIf here to prevent race condition with de-dup logic in Add method.
	_ = p.mapToAckChan.RemoveIf(visibilityTaskKey, func(key interface{}, value interface{}) bool {
		ackCh, ok := value.(*ackChan)
		if !ok {
			p.logger.Fatal(fmt.Sprintf("mapToAckChan has item of a wrong type %T (%T expected).", value, &ackChan{}), tag.ESKey(visibilityTaskKey))
		}

		ackCh.done(ack, p.metricsClient)
		return true
	})
}

func (p *processorImpl) extractVisibilityTaskKey(request elastic.BulkableRequest) string {
	req, err := request.Source()
	if err != nil {
		p.logger.Error("Unable to get ES request source.", tag.Error(err), tag.ESRequest(request.String()))
		p.metricsClient.IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCorruptedData)
		return ""
	}

	if len(req) == 2 { // index or update requests
		var body map[string]interface{}
		if err = json.Unmarshal([]byte(req[1]), &body); err != nil {
			p.logger.Error("Unable to unmarshal ES request body.", tag.Error(err))
			p.metricsClient.IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCorruptedData)
			return ""
		}

		k, ok := body[searchattribute.VisibilityTaskKey]
		if !ok {
			p.logger.Error("Unable to extract VisibilityTaskKey from ES request.", tag.ESRequest(request.String()))
			p.metricsClient.IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCorruptedData)
			return ""
		}
		return k.(string)
	} else { // delete requests
		return p.extractDocID(request)
	}
}

func (p *processorImpl) extractDocID(request elastic.BulkableRequest) string {
	req, err := request.Source()
	if err != nil {
		p.logger.Error("Unable to get ES request source.", tag.Error(err), tag.ESRequest(request.String()))
		p.metricsClient.IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCorruptedData)
		return ""
	}

	var body map[string]map[string]interface{}
	if err = json.Unmarshal([]byte(req[0]), &body); err != nil {
		p.logger.Error("Unable to unmarshal ES request body.", tag.Error(err), tag.ESRequest(request.String()))
		p.metricsClient.IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCorruptedData)
		return ""
	}

	// There should be only one operation "index" or "delete".
	for _, opMap := range body {
		_id, ok := opMap["_id"]
		if ok {
			return _id.(string)
		}
	}

	p.logger.Error("Unable to extract _id from ES request.", tag.ESRequest(request.String()))
	p.metricsClient.IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCorruptedData)
	return ""
}

func isSuccess(item *elastic.BulkResponseItem) bool {
	if item.Status >= 200 && item.Status < 300 {
		return true
	}

	// Ignore version conflict.
	if item.Status == 409 {
		return true
	}

	if item.Status == 404 {
		if item.Error != nil && item.Error.Type == "index_not_found_exception" {
			return false
		}

		// Ignore document not found during delete operation.
		return true
	}

	return false
}

func extractErrorReason(resp *elastic.BulkResponseItem) string {
	if resp.Error != nil {
		return resp.Error.Reason
	}
	return ""
}

func newAckChan() *ackChan {
	var addedAt atomic.Value
	addedAt.Store(time.Time{})
	return &ackChan{
		ackChInternal: make(chan bool, 1),
		createdAt:     time.Now().UTC(),
		addedAt:       addedAt,
	}
}

func (a *ackChan) recordAdd(metricsClient metrics.Client) {
	addedAt := time.Now().UTC()
	a.addedAt.Store(addedAt)
	metricsClient.RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitAddLatency, addedAt.Sub(a.createdAt))
}

func (a *ackChan) recordStart(metricsClient metrics.Client) {
	a.startedAt = time.Now().UTC()
	addedAt := a.addedAt.Load().(time.Time)
	if !addedAt.IsZero() {
		metricsClient.RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitStartLatency, a.startedAt.Sub(addedAt))
	}
}

func (a *ackChan) done(ack bool, metricsClient metrics.Client) {
	select {
	case a.ackChInternal <- ack:
		doneAt := time.Now().UTC()
		if !a.createdAt.IsZero() {
			metricsClient.RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequestLatency, doneAt.Sub(a.createdAt))
		}
		if !a.startedAt.IsZero() {
			metricsClient.RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCommitLatency, doneAt.Sub(a.startedAt))
		}
	default:
		metricsClient.IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorDeadlock)
	}
}
