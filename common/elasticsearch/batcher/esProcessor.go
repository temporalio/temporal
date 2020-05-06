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

package esbatcher

import (
        "context"
        "encoding/json"
        "fmt"
        "time"

        "github.com/olivere/elastic"
        "github.com/uber-go/tally"

        "github.com/temporalio/temporal/common"
        "github.com/temporalio/temporal/common/collection"
        "github.com/temporalio/temporal/common/definition"
        es "github.com/temporalio/temporal/common/elasticsearch"
        "github.com/temporalio/temporal/common/log"
        "github.com/temporalio/temporal/common/log/tag"
        "github.com/temporalio/temporal/common/metrics"
        "github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (

        // Config contains all configs for indexer
        Config struct {
                IndexerConcurrency       dynamicconfig.IntPropertyFn
                ESProcessorNumOfWorkers  dynamicconfig.IntPropertyFn
                ESProcessorBulkActions   dynamicconfig.IntPropertyFn // max number of requests in bulk
                ESProcessorBulkSize      dynamicconfig.IntPropertyFn // max total size of bytes in bulk
                ESProcessorFlushInterval dynamicconfig.DurationPropertyFn
                ValidSearchAttributes    dynamicconfig.MapPropertyFn
        }

        // ESProcessor is interface for elastic search bulk processor
        ESProcessor interface {
                // Stop processor and clean up
                Stop()
                // Add request for upserting to bulk request and block until request is flushed to ES
                AddUpsert(domainID string, wid, rid string, workflowTypeName string,
                        startTimeUnixNano, executionTimeUnixNano int64, taskID int64, memo []byte,
                        encoding common.EncodingType, searchAttributes map[string][]byte) error
                // Add request for deletion to bulk request and block until request is flushed to ES

                AddDelete(domainID string, wid, rid string, taskID int64) error
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

        // esBatchUpdaterImpl implements ESProcessor, it's an agent of elastic.BulkProcessor
        esBatchUpdaterImpl struct {
                processor     ElasticBulkProcessor
                keyToNotifier collection.ConcurrentTxMap // used to map ES request to kafka message
                config        *Config
                logger        log.Logger
                metricsClient metrics.Client
                esIndexName   string
        }

        insertNotifier struct { // value of esBatchUpdaterImpl.keyToNotifier
                inserted       chan struct{}    // chan used to indicate that the request has been inserted
                insertErr      error            // a potential error
                swFromAddToAck *tally.Stopwatch // metric from message add to message inserted
        }
)

var _ ESProcessor = (*esBatchUpdaterImpl)(nil)
var _ ElasticBulkProcessor = (*elastic.BulkProcessor)(nil)

const (
        // retry configs for es bulk processor
        esProcessorInitialRetryInterval = 200 * time.Millisecond
        esProcessorMaxRetryInterval     = 20 * time.Second

        esDocIDDelimiter = "~"
        esDocType        = "_doc"

        versionTypeExternal = "external"
)

// NewESProcessorAndStart create new ESProcessor and start
func NewESProcessorAndStart(config *Config, client es.Client, processorName string,
        logger log.Logger, metricsClient metrics.Client) (ESProcessor, error) {
        p := &esBatchUpdaterImpl{
                config:        config,
                logger:        logger.WithTags(tag.ComponentIndexerESProcessor),
                metricsClient: metricsClient,
        }

        // TODO becker probably need to introduce a new config struct for these params
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
        // TODO becker can I still use this hash function?
        p.keyToNotifier = collection.NewShardedConcurrentTxMap(1024, p.hashFn)
        return p, nil
}

func (p *esBatchUpdaterImpl) Stop() {
        p.processor.Stop() //nolint:errcheck
        p.keyToNotifier = nil
}

// Add an ES request, and an map item for kafka message
func (p *esBatchUpdaterImpl) AddUpsert(domainID string, wid, rid string, workflowTypeName string,
        startTimeUnixNano, executionTimeUnixNano int64, taskID int64, memo []byte,
        encoding common.EncodingType, searchAttributes map[string][]byte) error {

        docID := getDocID(wid, rid)
        doc := p.buildESDoc(domainID, wid, rid, workflowTypeName, startTimeUnixNano,
                executionTimeUnixNano, memo, encoding, searchAttributes)
        request := elastic.NewBulkIndexRequest().
                Index(p.esIndexName).
                Type(esDocType).
                Id(docID).
                VersionType(versionTypeExternal).
                Version(taskID).
                Doc(doc)

        return p.addRequest(docID, request)
}

func (p *esBatchUpdaterImpl) AddDelete(domainID string, wid, rid string, taskID int64) error {
        docID := getDocID(wid, rid)
        request := elastic.NewBulkDeleteRequest().
                Index(p.esIndexName).
                Type(esDocType).
                Id(docID).
                VersionType(versionTypeExternal).
                Version(taskID)
        return p.addRequest(docID, request)
}

func (p *esBatchUpdaterImpl) addRequest(docID string, request elastic.BulkableRequest) error {
        actionWhenFoundDuplicates := func(key interface{}, value interface{}) error {
                // TODO becker we can just do nothing here, right?
                return nil
        }
        sw := p.metricsClient.StartTimer(metrics.ESProcessorScope, metrics.ESProcessorProcessMsgLatency)
        mapVal := newInsertNotifier(&sw)
        _, isDup, _ := p.keyToNotifier.PutOrDo(docID, mapVal, actionWhenFoundDuplicates)
        if isDup {
                return nil
        }
        p.processor.Add(request)
        return mapVal.blockUntilInserted()
}

func getDocID(workflowID, runID string) string {
        return workflowID + esDocIDDelimiter + runID
}

func (p *esBatchUpdaterImpl) decodeSearchAttrBinary(bytes []byte, key string) interface{} {
        var val interface{}
        err := json.Unmarshal(bytes, &val)
        if err != nil {
                p.logger.Error("Error when decode search attributes values.", tag.Error(err), tag.ESField(key))
                p.metricsClient.IncCounter(metrics.IndexProcessorScope, metrics.IndexProcessorCorruptedData)
        }
        return val
}

func (p *esBatchUpdaterImpl) buildESDoc(domainID, workflowID, runID, workflowTypeName string, startTimeUnixNano,
        executionTimeUnixNano int64, memo []byte, encoding common.EncodingType, searchAttributes map[string][]byte) map[string]interface{} {
        doc := make(map[string]interface{})
        doc[definition.DomainID] = domainID
        doc[definition.WorkflowID] = workflowID
        doc[definition.RunID] = runID
        doc[es.WorkflowType] = workflowTypeName
        doc[es.StartTime] = startTimeUnixNano
        doc[es.ExecutionTime] = executionTimeUnixNano
        if len(memo) != 0 {
                doc[es.Memo] = memo
                doc[es.Encoding] = encoding
        }

        attr := make(map[string]interface{})
        for k, v := range searchAttributes {
                if !p.isValidFieldToES(k) {
                        p.logger.Error("Unregistered field.", tag.ESField(k))
                        p.metricsClient.IncCounter(metrics.IndexProcessorScope, metrics.IndexProcessorCorruptedData)
                        continue
                }
                attr[k] = p.decodeSearchAttrBinary(v, k)
        }
        doc[definition.Attr] = attr
        return doc
}

func (p *esBatchUpdaterImpl) isValidFieldToES(field string) bool {
        if _, ok := p.config.ValidSearchAttributes()[field]; ok {
                return true
        }
        if field == definition.Memo || field == definition.KafkaKey || field == definition.Encoding {
                return true
        }
        return false
}

func newInsertNotifier(stopwatch *tally.Stopwatch) *insertNotifier {
        return &insertNotifier{
                inserted:       make(chan struct{}),
                insertErr:      nil,
                swFromAddToAck: stopwatch,
        }
}

func (i *insertNotifier) blockUntilInserted() error {
        <-i.inserted
        return i.insertErr
}

func (i *insertNotifier) success() {
        close(i.inserted)
}

func (i *insertNotifier) err(e error) {
        i.insertErr = e
        close(i.inserted)
}

// bulkBeforeAction is triggered before bulk processor commit
func (p *esBatchUpdaterImpl) bulkBeforeAction(executionID int64, requests []elastic.BulkableRequest) {
        p.metricsClient.AddCounter(metrics.ESProcessorScope, metrics.ESProcessorRequests, int64(len(requests)))
}

// bulkAfterAction is triggered after bulk processor commit
func (p *esBatchUpdaterImpl) bulkAfterAction(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
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
                                p.unblockInsert(key)
                        case !isResponseRetriable(resp.Status):
                                p.logger.Error("ES request failed.",
                                        tag.ESResponseStatus(resp.Status), tag.ESResponseError(getErrorMsgFromESResp(resp)))
                                p.failInsert(key, fmt.Errorf("Failed to insert to ES. Status: %s, Error: %s",
                                        resp.Status, getErrorMsgFromESResp(resp)))
                        default: // bulk processor will retry
                                p.logger.Info("ES request retried.", tag.ESResponseStatus(resp.Status))
                                p.metricsClient.IncCounter(metrics.ESProcessorScope, metrics.ESProcessorRetries)
                        }
                }
        }
}

func (p *esBatchUpdaterImpl) unblockInsert(key string) {
        v, ok := p.keyToNotifier.Get(key)
        if !ok {
                p.logger.Error(fmt.Sprintf("Unable to find notifier for key %s", key))
                return
        }

        notifier := v.(insertNotifier)
        notifier.success()
        notifier.swFromAddToAck.Stop()
}

func (p *esBatchUpdaterImpl) failInsert(key string, err error) {
        v, ok := p.keyToNotifier.Get(key)
        if !ok {
                p.logger.Error(fmt.Sprintf("Unable to find notifier for key %s", key))
                return
        }

        notifier := v.(insertNotifier)
        notifier.err(err)
        notifier.swFromAddToAck.Stop()
}

func (p *esBatchUpdaterImpl) hashFn(key interface{}) uint32 {
        id, ok := key.(string)
        if !ok {
                return 0
        }
        numOfShards := p.config.IndexerConcurrency()
        return uint32(common.WorkflowIDToHistoryShard(id, numOfShards))
}

func (p *esBatchUpdaterImpl) getKeyForKafkaMsg(request elastic.BulkableRequest) string {
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

