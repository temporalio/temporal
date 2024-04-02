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

// TODO: enable this after https://github.com/golang/mock/issues/621
// mockgen -copyright_file ../../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination processor_mock.go

package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/olivere/elastic/v7"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// Processor is interface for Elasticsearch bulk processor
	Processor interface {
		// Add request to bulk processor.
		Add(request *client.BulkableRequest, visibilityTaskKey string) *future.FutureImpl[bool]
		Start()
		Stop()
	}

	// processorImpl implements Processor, it's an agent of elastic.BulkProcessor
	processorImpl struct {
		status                  int32
		bulkProcessor           client.BulkProcessor
		bulkProcessorParameters *client.BulkProcessorParameters
		client                  client.Client
		mapToAckFuture          collection.ConcurrentTxMap // used to map ES request to ack channel
		logger                  log.Logger
		metricsHandler          metrics.Handler
		indexerConcurrency      uint32
		shutdownLock            sync.RWMutex
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

	ackFuture struct { // value of processorImpl.mapToAckFuture
		future    *future.FutureImpl[bool]
		createdAt time.Time    // Time when request was created (used to report metrics).
		addedAt   atomic.Value // of time.Time // Time when request was added to bulk processor (used to report metrics).
		startedAt time.Time    // Time when request was sent to Elasticsearch by bulk processor (used to report metrics).
	}
)

var _ Processor = (*processorImpl)(nil)

const (
	visibilityProcessorName = "visibility-processor"
)

var (
	errVisibilityShutdown = errors.New("visiblity processor was shut down")
)

// NewProcessor create new processorImpl
func NewProcessor(
	cfg *ProcessorConfig,
	esClient client.Client,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *processorImpl {

	p := &processorImpl{
		status:             common.DaemonStatusInitialized,
		client:             esClient,
		logger:             log.With(logger, tag.ComponentIndexerESProcessor),
		metricsHandler:     metricsHandler.WithTags(metrics.OperationTag(metrics.ElasticsearchBulkProcessor)),
		indexerConcurrency: uint32(cfg.IndexerConcurrency()),
		bulkProcessorParameters: &client.BulkProcessorParameters{
			Name:          visibilityProcessorName,
			NumOfWorkers:  cfg.ESProcessorNumOfWorkers(),
			BulkActions:   cfg.ESProcessorBulkActions(),
			BulkSize:      cfg.ESProcessorBulkSize(),
			FlushInterval: cfg.ESProcessorFlushInterval(),
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
	p.mapToAckFuture = collection.NewShardedConcurrentTxMap(1024, p.hashFn)
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

	p.shutdownLock.Lock()
	defer p.shutdownLock.Unlock()

	err := p.bulkProcessor.Stop()
	if err != nil {
		// This could happen if ES is down when we're trying to shut down the server.
		p.logger.Error("Unable to stop Elasticsearch processor.", tag.LifeCycleStopFailed, tag.Error(err))
		return
	}
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

// Add request to the bulk and return a future object which will receive ack signal when request is processed.
func (p *processorImpl) Add(request *client.BulkableRequest, visibilityTaskKey string) *future.FutureImpl[bool] {
	newFuture := newAckFuture() // Create future first to measure impact of following RWLock on latency

	p.shutdownLock.RLock()
	defer p.shutdownLock.RUnlock()

	if atomic.LoadInt32(&p.status) == common.DaemonStatusStopped {
		p.logger.Warn("Rejecting ES request for visibility task key because processor has been shut down.", tag.Key(visibilityTaskKey), tag.ESDocID(request.ID), tag.Value(request.Doc))
		newFuture.future.Set(false, errVisibilityShutdown)
		return newFuture.future
	}

	_, isDup, _ := p.mapToAckFuture.PutOrDo(visibilityTaskKey, newFuture, func(key interface{}, value interface{}) error {
		existingFuture, ok := value.(*ackFuture)
		if !ok {
			p.logger.Fatal(fmt.Sprintf("mapToAckFuture has item of a wrong type %T (%T expected).", value, &ackFuture{}), tag.Value(key))
		}

		p.logger.Warn("Skipping duplicate ES request for visibility task key.", tag.Key(visibilityTaskKey), tag.ESDocID(request.ID), tag.Value(request.Doc), tag.NewDurationTag("interval-between-duplicates", newFuture.createdAt.Sub(existingFuture.createdAt)))
		metrics.ElasticsearchBulkProcessorDuplicateRequest.With(p.metricsHandler).Record(1)
		newFuture = existingFuture
		return nil
	})
	if !isDup {
		p.bulkProcessor.Add(request)
		newFuture.recordAdd(p.metricsHandler)
	}
	return newFuture.future
}

// bulkBeforeAction is triggered before bulk processor commit
func (p *processorImpl) bulkBeforeAction(_ int64, requests []elastic.BulkableRequest) {
	metrics.ElasticsearchBulkProcessorRequests.With(p.metricsHandler).Record(int64(len(requests)))
	p.metricsHandler.Histogram(metrics.ElasticsearchBulkProcessorBulkSize.Name(), metrics.ElasticsearchBulkProcessorBulkSize.Unit()).
		Record(int64(len(requests)))

	for _, request := range requests {
		visibilityTaskKey := p.extractVisibilityTaskKey(request)
		if visibilityTaskKey == "" {
			continue
		}
		_, _, _ = p.mapToAckFuture.GetAndDo(visibilityTaskKey, func(key interface{}, value interface{}) error {
			ackF, ok := value.(*ackFuture)
			if !ok {
				p.logger.Fatal(fmt.Sprintf("mapToAckFuture has item of a wrong type %T (%T expected).", value, &ackFuture{}), tag.Value(key))
			}
			ackF.recordStart(p.metricsHandler)
			return nil
		})
	}
}

// bulkAfterAction is triggered after bulk processor commit
func (p *processorImpl) bulkAfterAction(_ int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		const logFirstNRequests = 5
		var httpStatus int
		var esErr *elastic.Error
		if errors.As(err, &esErr) {
			httpStatus = esErr.Status
		}

		var logRequests strings.Builder
		for i, request := range requests {
			if i < logFirstNRequests {
				logRequests.WriteString(request.String())
				logRequests.WriteRune('\n')
			}
			metrics.ElasticsearchBulkProcessorFailures.With(p.metricsHandler).Record(1, metrics.HttpStatusTag(httpStatus))
			visibilityTaskKey := p.extractVisibilityTaskKey(request)
			if visibilityTaskKey == "" {
				continue
			}
			p.notifyResult(visibilityTaskKey, false)
		}
		p.logger.Error("Unable to commit bulk ES request.", tag.Error(err), tag.RequestCount(len(requests)), tag.ESRequest(logRequests.String()))
		return
	}

	// Record how long the Elasticsearch took to process the bulk request.
	metrics.ElasticsearchBulkProcessorBulkResquestTookLatency.With(p.metricsHandler).
		Record(time.Duration(response.Took) * time.Millisecond)

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
			metrics.ElasticsearchBulkProcessorCorruptedData.With(p.metricsHandler).Record(1)
			p.notifyResult(visibilityTaskKey, false)
			continue
		}

		if !isSuccess(responseItem) {
			p.logger.Error("ES request failed.",
				tag.ESResponseStatus(responseItem.Status),
				tag.ESResponseError(extractErrorReason(responseItem)),
				tag.Key(visibilityTaskKey),
				tag.ESDocID(docID),
				tag.ESRequest(request.String()))
			metrics.ElasticsearchBulkProcessorFailures.With(p.metricsHandler).Record(1, metrics.HttpStatusTag(responseItem.Status))
			p.notifyResult(visibilityTaskKey, false)
			continue
		}

		p.notifyResult(visibilityTaskKey, true)
	}

	// Record how many documents are waiting to be flushed to Elasticsearch after this bulk is committed.
	p.metricsHandler.Histogram(metrics.ElasticsearchBulkProcessorQueuedRequests.Name(), metrics.ElasticsearchBulkProcessorBulkSize.Unit()).
		Record(int64(p.mapToAckFuture.Len()))
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

func (p *processorImpl) notifyResult(visibilityTaskKey string, ack bool) {
	// Use RemoveIf here to prevent race condition with de-dup logic in Add method.
	_ = p.mapToAckFuture.RemoveIf(visibilityTaskKey, func(key interface{}, value interface{}) bool {
		ackF, ok := value.(*ackFuture)
		if !ok {
			p.logger.Fatal(fmt.Sprintf("mapToAckFuture has item of a wrong type %T (%T expected).", value, &ackFuture{}), tag.ESKey(visibilityTaskKey))
		}

		ackF.done(ack, p.metricsHandler)
		return true
	})
}

func (p *processorImpl) extractVisibilityTaskKey(request elastic.BulkableRequest) string {
	req, err := request.Source()
	if err != nil {
		p.logger.Error("Unable to get ES request source.", tag.Error(err), tag.ESRequest(request.String()))
		metrics.ElasticsearchBulkProcessorCorruptedData.With(p.metricsHandler).Record(1)
		return ""
	}

	if len(req) == 2 { // index or update requests
		var body map[string]interface{}
		if err = json.Unmarshal([]byte(req[1]), &body); err != nil {
			p.logger.Error("Unable to unmarshal ES request body.", tag.Error(err))
			metrics.ElasticsearchBulkProcessorCorruptedData.With(p.metricsHandler).Record(1)
			return ""
		}

		k, ok := body[searchattribute.VisibilityTaskKey]
		if !ok {
			p.logger.Error("Unable to extract VisibilityTaskKey from ES request.", tag.ESRequest(request.String()))
			metrics.ElasticsearchBulkProcessorCorruptedData.With(p.metricsHandler).Record(1)
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
		metrics.ElasticsearchBulkProcessorCorruptedData.With(p.metricsHandler).Record(1)

		return ""
	}

	var body map[string]map[string]interface{}
	if err = json.Unmarshal([]byte(req[0]), &body); err != nil {
		p.logger.Error("Unable to unmarshal ES request body.", tag.Error(err), tag.ESRequest(request.String()))
		metrics.ElasticsearchBulkProcessorCorruptedData.With(p.metricsHandler).Record(1)
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
	metrics.ElasticsearchBulkProcessorCorruptedData.With(p.metricsHandler).Record(1)
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

func newAckFuture() *ackFuture {
	var addedAt atomic.Value
	addedAt.Store(time.Time{})
	return &ackFuture{
		future:    future.NewFuture[bool](),
		createdAt: time.Now().UTC(),
		addedAt:   addedAt,
	}
}

func (a *ackFuture) recordAdd(metricsHandler metrics.Handler) {
	addedAt := time.Now().UTC()
	a.addedAt.Store(addedAt)
	metrics.ElasticsearchBulkProcessorWaitAddLatency.With(metricsHandler).Record(addedAt.Sub(a.createdAt))
}

func (a *ackFuture) recordStart(metricsHandler metrics.Handler) {
	a.startedAt = time.Now().UTC()
	addedAt := a.addedAt.Load().(time.Time)
	if !addedAt.IsZero() {
		metrics.ElasticsearchBulkProcessorWaitStartLatency.With(metricsHandler).Record(a.startedAt.Sub(addedAt))
	}
}

func (a *ackFuture) done(ack bool, metricsHandler metrics.Handler) {
	a.future.Set(ack, nil)
	doneAt := time.Now().UTC()
	if !a.createdAt.IsZero() {
		metrics.ElasticsearchBulkProcessorRequestLatency.With(metricsHandler).Record(doneAt.Sub(a.createdAt))
	}
	if !a.startedAt.IsZero() {
		metrics.ElasticsearchBulkProcessorCommitLatency.With(metricsHandler).Record(doneAt.Sub(a.startedAt))
	}
}
