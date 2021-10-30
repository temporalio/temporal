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

package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
)

type processorSuite struct {
	suite.Suite
	controller        *gomock.Controller
	esProcessor       *processorImpl
	mockBulkProcessor *client.MockBulkProcessor
	mockMetricClient  *metrics.MockClient
	mockESClient      *client.MockClient
}

var (
	testID = "test-doc-id"
)

func TestElasticsearchProcessorSuite(t *testing.T) {
	s := new(processorSuite)
	suite.Run(t, s)
}

func (s *processorSuite) SetupSuite() {
}

func (s *processorSuite) SetupTest() {
	logger := log.NewTestLogger()

	s.controller = gomock.NewController(s.T())

	cfg := &ProcessorConfig{
		IndexerConcurrency:       dynamicconfig.GetIntPropertyFn(32),
		ESProcessorNumOfWorkers:  dynamicconfig.GetIntPropertyFn(1),
		ESProcessorBulkActions:   dynamicconfig.GetIntPropertyFn(10),
		ESProcessorBulkSize:      dynamicconfig.GetIntPropertyFn(2 << 20),
		ESProcessorFlushInterval: dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
	}

	s.mockMetricClient = metrics.NewMockClient(s.controller)
	s.mockBulkProcessor = client.NewMockBulkProcessor(s.controller)
	s.mockESClient = client.NewMockClient(s.controller)
	s.esProcessor = NewProcessor(cfg, s.mockESClient, logger, s.mockMetricClient)

	// esProcessor.Start mock
	s.esProcessor.mapToAckChan = collection.NewShardedConcurrentTxMap(1024, s.esProcessor.hashFn)
	s.esProcessor.bulkProcessor = s.mockBulkProcessor
}

func (s *processorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *processorSuite) TestNewESProcessorAndStartStop() {
	config := &ProcessorConfig{
		IndexerConcurrency:       dynamicconfig.GetIntPropertyFn(32),
		ESProcessorNumOfWorkers:  dynamicconfig.GetIntPropertyFn(1),
		ESProcessorBulkActions:   dynamicconfig.GetIntPropertyFn(10),
		ESProcessorBulkSize:      dynamicconfig.GetIntPropertyFn(2 << 20),
		ESProcessorFlushInterval: dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
	}

	p := NewProcessor(config, s.mockESClient, s.esProcessor.logger, s.mockMetricClient)

	s.mockESClient.EXPECT().RunBulkProcessor(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *client.BulkProcessorParameters) (client.BulkProcessor, error) {
			s.Equal(visibilityProcessorName, input.Name)
			s.Equal(config.ESProcessorNumOfWorkers(), input.NumOfWorkers)
			s.Equal(config.ESProcessorBulkActions(), input.BulkActions)
			s.Equal(config.ESProcessorBulkSize(), input.BulkSize)
			s.Equal(config.ESProcessorFlushInterval(), input.FlushInterval)
			s.NotNil(input.Backoff)
			s.NotNil(input.AfterFunc)

			bulkProcessor := client.NewMockBulkProcessor(s.controller)
			bulkProcessor.EXPECT().Stop()
			return bulkProcessor, nil
		}).
		Times(1)

	p.Start()
	s.NotNil(p.mapToAckChan)
	s.NotNil(p.bulkProcessor)

	p.Stop()
	s.Nil(p.mapToAckChan)
	s.Nil(p.bulkProcessor)
}

func (s *processorSuite) TestAdd() {
	request := &client.BulkableRequest{}
	visibilityTaskKey := "test-key"
	s.Equal(0, s.esProcessor.mapToAckChan.Len())

	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitAddLatency, gomock.Any())
	s.mockBulkProcessor.EXPECT().Add(request)

	ackCh1 := s.esProcessor.Add(request, visibilityTaskKey)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())
	select {
	case <-ackCh1:
		s.Fail("1st request shouldn't be acknowledged")
	default:
	}

	// handle duplicate
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorDuplicateRequest)
	ackCh2 := s.esProcessor.Add(request, visibilityTaskKey)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())
	select {
	case ack := <-ackCh2:
		s.True(ack)
	default:
		s.Fail("2nd (new) request should be acked due to duplicate key")
	}

	select {
	case <-ackCh1:
		s.Fail("1st (existing) request shouldn't be acknowledged")
	default:
	}
}

func (s *processorSuite) TestAdd_ConcurrentAdd() {
	request := &client.BulkableRequest{}
	docsCount := 1000
	parallelFactor := 10
	ackChs := make([]<-chan bool, docsCount)

	wg := sync.WaitGroup{}
	wg.Add(parallelFactor)
	s.mockBulkProcessor.EXPECT().Add(request).Times(docsCount)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitAddLatency, gomock.Any()).Times(docsCount)
	for i := 0; i < parallelFactor; i++ {
		go func(i int) {
			for j := 0; j < docsCount/parallelFactor; j++ {
				ackChs[i*docsCount/parallelFactor+j] = s.esProcessor.Add(request, fmt.Sprintf("test-key-%d-%d", i, j))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	s.Equal(docsCount, s.esProcessor.mapToAckChan.Len())

	for i := 0; i < docsCount; i++ {
		select {
		case <-ackChs[i]:
			s.Fail("all request must be in the bulk")
		default:
		}
	}
}

func (s *processorSuite) TestAdd_ConcurrentAdd_Duplicates() {
	request := &client.BulkableRequest{}
	key := "test-key"
	duplicates := 100
	ackChs := make([]<-chan bool, duplicates)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitAddLatency, gomock.Any()).Times(1)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any()).Times(duplicates - 1)
	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorDuplicateRequest).Times(duplicates - 1)

	wg := sync.WaitGroup{}
	wg.Add(duplicates)
	s.mockBulkProcessor.EXPECT().Add(request)
	for i := 0; i < duplicates; i++ {
		go func(i int) {
			ackChs[i] = s.esProcessor.Add(request, key)
			wg.Done()
		}(i)
	}
	wg.Wait()
	pendingRequestsCount := 0
	for i := 0; i < duplicates; i++ {
		select {
		case ack := <-ackChs[i]:
			s.True(ack)
		default:
			pendingRequestsCount++
		}
	}

	s.Equal(1, pendingRequestsCount, "only one request should not be acked")
	s.Equal(1, s.esProcessor.mapToAckChan.Len(), "only one request should be in the bulk")
}

func (s *processorSuite) TestBulkAfterAction_Ack() {
	version := int64(3)
	testKey := "testKey"
	request := elastic.NewBulkIndexRequest().
		Index(testIndex).
		Id(testID).
		Version(version).
		Doc(map[string]interface{}{searchattribute.VisibilityTaskKey: testKey})
	requests := []elastic.BulkableRequest{request}

	mSuccess := map[string]*elastic.BulkResponseItem{
		"index": {
			Index:   testIndex,
			Id:      testID,
			Version: version,
			Status:  200,
		},
	}
	response := &elastic.BulkResponse{
		Took:   3,
		Errors: false,
		Items:  []map[string]*elastic.BulkResponseItem{mSuccess},
	}

	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
	mapVal := newAckChan()
	s.esProcessor.mapToAckChan.Put(testKey, mapVal)
	s.esProcessor.bulkAfterAction(0, requests, response, nil)
	select {
	case ack := <-mapVal.ackChInternal:
		s.True(ack)
	default:
		s.Fail("request should be acknowledged")
	}
}

func (s *processorSuite) TestBulkAfterAction_Nack() {
	version := int64(3)
	testKey := "testKey"

	wid := "test-workflowID"
	rid := "test-runID"
	namespaceID := "test-namespaceID"

	request := elastic.NewBulkIndexRequest().
		Index(testIndex).
		Id(testID).
		Version(version).
		Doc(map[string]interface{}{
			searchattribute.VisibilityTaskKey: testKey,
			searchattribute.NamespaceID:       namespaceID,
			searchattribute.WorkflowID:        wid,
			searchattribute.RunID:             rid,
		})
	requests := []elastic.BulkableRequest{request}

	mFailed := map[string]*elastic.BulkResponseItem{
		"index": {
			Index:   testIndex,
			Id:      testID,
			Version: version,
			Status:  400,
		},
	}
	response := &elastic.BulkResponse{
		Took:   3,
		Errors: false,
		Items:  []map[string]*elastic.BulkResponseItem{mFailed},
	}

	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
	mapVal := newAckChan()
	s.esProcessor.mapToAckChan.Put(testKey, mapVal)
	s.mockMetricClient.EXPECT().Scope(metrics.ElasticsearchBulkProcessor, metrics.HttpStatusTag(400)).Return(metrics.NoopScope(0))
	s.esProcessor.bulkAfterAction(0, requests, response, nil)
	select {
	case ack := <-mapVal.ackChInternal:
		s.False(ack)
	default:
		s.Fail("request should be acknowledged")
	}
}

func (s *processorSuite) TestBulkAfterAction_Error() {
	version := int64(3)
	doc := map[string]interface{}{
		searchattribute.VisibilityTaskKey: "str",
	}

	request := elastic.NewBulkIndexRequest().
		Index(testIndex).
		Id(testID).
		Version(version).
		Doc(doc)
	requests := []elastic.BulkableRequest{request}

	mFailed := map[string]*elastic.BulkResponseItem{
		"index": {
			Index:   testIndex,
			Id:      testID,
			Version: version,
			Status:  400,
		},
	}
	response := &elastic.BulkResponse{
		Took:   3,
		Errors: true,
		Items:  []map[string]*elastic.BulkResponseItem{mFailed},
	}

	s.mockMetricClient.EXPECT().Scope(metrics.ElasticsearchBulkProcessor, metrics.HttpStatusTag(400)).Return(metrics.NoopScope(0))
	s.esProcessor.bulkAfterAction(0, requests, response, &elastic.Error{Status: 400})
}

func (s *processorSuite) TestBulkBeforeAction() {
	version := int64(3)
	testKey := "testKey"
	request := elastic.NewBulkIndexRequest().
		Index(testIndex).
		Id(testID).
		Version(version).
		Doc(map[string]interface{}{searchattribute.VisibilityTaskKey: testKey})
	requests := []elastic.BulkableRequest{request}

	s.mockMetricClient.EXPECT().AddCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequests, int64(1))
	s.mockMetricClient.EXPECT().RecordDistribution(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorBulkSize, 1)
	s.mockMetricClient.EXPECT().RecordDistribution(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorQueuedRequests, 0)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitAddLatency, gomock.Any())
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitStartLatency, gomock.Any())

	mapVal := newAckChan()
	mapVal.recordAdd(s.mockMetricClient)
	s.esProcessor.mapToAckChan.Put(testKey, mapVal)
	s.True(mapVal.startedAt.IsZero())
	s.esProcessor.bulkBeforeAction(0, requests)
	s.False(mapVal.startedAt.IsZero())
}

func (s *processorSuite) TestAckChan() {
	key := "test-key"
	// no msg in map, nothing called
	s.esProcessor.sendToAckChan(key, true)

	request := &client.BulkableRequest{}
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitAddLatency, gomock.Any())
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
	s.mockBulkProcessor.EXPECT().Add(request)
	ackCh := s.esProcessor.Add(request, key)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())

	s.esProcessor.sendToAckChan(key, true)
	select {
	case ack := <-ackCh:
		s.True(ack)
	default:
		s.Fail("request should be acknowledged")
	}
	s.Equal(0, s.esProcessor.mapToAckChan.Len())
}

func (s *processorSuite) TestNackChan() {
	key := "test-key-nack"
	// no msg in map, nothing called
	s.esProcessor.sendToAckChan(key, false)

	request := &client.BulkableRequest{}
	s.mockBulkProcessor.EXPECT().Add(request)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitAddLatency, gomock.Any())
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
	ackCh := s.esProcessor.Add(request, key)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())

	s.esProcessor.sendToAckChan(key, false)
	select {
	case ack := <-ackCh:
		s.False(ack)
	default:
		s.Fail("request should be not acknowledged")
	}
	s.Equal(0, s.esProcessor.mapToAckChan.Len())
}

func (s *processorSuite) TestHashFn() {
	s.Equal(uint32(0), s.esProcessor.hashFn(0))
	s.NotEqual(uint32(0), s.esProcessor.hashFn("test"))
}

func (s *processorSuite) TestExtractVisibilityTaskKey() {
	request := elastic.NewBulkIndexRequest()
	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCorruptedData)

	visibilityTaskKey := s.esProcessor.extractVisibilityTaskKey(request)
	s.Equal("", visibilityTaskKey)

	m := map[string]interface{}{
		searchattribute.VisibilityTaskKey: 1,
	}
	request.Doc(m)
	s.Panics(func() { s.esProcessor.extractVisibilityTaskKey(request) })

	testKey := "test-key"
	m[searchattribute.VisibilityTaskKey] = testKey
	request.Doc(m)
	s.Equal(testKey, s.esProcessor.extractVisibilityTaskKey(request))
}

func (s *processorSuite) TestExtractVisibilityTaskKey_Delete() {
	request := elastic.NewBulkDeleteRequest()

	// ensure compatible with dependency
	source, err := request.Source()
	s.NoError(err)
	s.Equal(1, len(source))
	var body map[string]map[string]interface{}
	err = json.Unmarshal([]byte(source[0]), &body)
	s.NoError(err)
	_, ok := body["delete"]
	s.True(ok)

	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCorruptedData)
	key := s.esProcessor.extractVisibilityTaskKey(request)
	s.Equal("", key)

	id := "id"
	request.Id(id)
	key = s.esProcessor.extractVisibilityTaskKey(request)
	s.Equal(id, key)
}

func (s *processorSuite) TestIsResponseSuccess() {
	item := &elastic.BulkResponseItem{}

	for status := 200; status < 300; status++ {
		item.Status = status
		s.True(isSuccess(item))
	}

	item.Status = 409
	s.True(isSuccess(item))
	item.Status = 404
	s.True(isSuccess(item))
	item.Error = &elastic.ErrorDetails{Type: "index_not_found_exception"}
	s.False(isSuccess(item))

	for _, status := range []int{100, 199, 300, 400, 500, 408, 429, 503, 507} {
		item.Status = status
		s.False(isSuccess(item))
	}
}

func (s *processorSuite) TestErrorReasonFromResponse() {
	reason := "error reason"
	resp := &elastic.BulkResponseItem{Status: 400}
	s.Equal("", extractErrorReason(resp))
	resp.Error = &elastic.ErrorDetails{Reason: reason}
	s.Equal(reason, extractErrorReason(resp))
}

func (s *processorSuite) Test_End2End() {
	docsCount := 1000
	parallelFactor := 10
	version := int64(2208) //random

	request := &client.BulkableRequest{}
	bulkIndexRequests := make([]elastic.BulkableRequest, docsCount)
	bulkIndexResponse := &elastic.BulkResponse{
		Took:   3,
		Errors: false,
		Items:  make([]map[string]*elastic.BulkResponseItem, docsCount),
	}
	ackChs := make([]<-chan bool, docsCount)

	// Add documents in parallel.
	wg := sync.WaitGroup{}
	wg.Add(parallelFactor)
	s.mockBulkProcessor.EXPECT().Add(request).Times(docsCount)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitAddLatency, gomock.Any()).Times(docsCount)
	for i := 0; i < parallelFactor; i++ {
		go func(i int) {
			for j := 0; j < docsCount/parallelFactor; j++ {
				docIndex := i*docsCount/parallelFactor + j
				testKey := fmt.Sprintf("test-key-%d-%d", i, j)
				docId := fmt.Sprintf("docId-%d", docIndex)
				ackChs[docIndex] = s.esProcessor.Add(request, testKey)
				bulkIndexRequests[docIndex] = elastic.NewBulkIndexRequest().
					Index(testIndex).
					Id(docId).
					Version(version).
					Doc(map[string]interface{}{searchattribute.VisibilityTaskKey: testKey})

				mSuccess := map[string]*elastic.BulkResponseItem{
					"index": {
						Index:   testIndex,
						Id:      docId,
						Version: version,
						Status:  200,
					},
				}
				bulkIndexResponse.Items[docIndex] = mSuccess
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	s.Equal(docsCount, s.esProcessor.mapToAckChan.Len())

	// Emulate bulk commit.

	s.mockMetricClient.EXPECT().AddCounter(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequests, int64(docsCount))
	s.mockMetricClient.EXPECT().RecordDistribution(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorQueuedRequests, 0)
	s.mockMetricClient.EXPECT().RecordDistribution(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorBulkSize, docsCount)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorWaitStartLatency, gomock.Any()).Times(docsCount)
	s.esProcessor.bulkBeforeAction(0, bulkIndexRequests)

	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any()).Times(docsCount)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchBulkProcessor, metrics.ElasticsearchBulkProcessorCommitLatency, gomock.Any()).Times(docsCount)
	s.esProcessor.bulkAfterAction(0, bulkIndexRequests, bulkIndexResponse, nil)

	for i := 0; i < docsCount; i++ {
		select {
		case ack := <-ackChs[i]:
			s.True(ack)
		default:
			s.Fail("all requests must be acked")
		}
	}
}
