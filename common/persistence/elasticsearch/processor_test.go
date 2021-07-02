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
	"errors"
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
	"go.temporal.io/server/common/persistence/elasticsearch/client"
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

	s.mockBulkProcessor.EXPECT().Add(request)

	ackCh1 := s.esProcessor.Add(request, visibilityTaskKey)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())
	select {
	case <-ackCh1:
		s.Fail("1st request shouldn't be acknowledged")
	default:
	}

	// handle duplicate
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
	ackCh2 := s.esProcessor.Add(request, visibilityTaskKey)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())
	select {
	case ack := <-ackCh1:
		s.False(ack)
	default:
		s.Fail("1st (existing) request should be nacked due to duplicate key")
	}

	select {
	case <-ackCh2:
		s.Fail("2nd request shouldn't be acknowledged")
	default:
	}
}

func (s *processorSuite) TestAdd_ConcurrentAdd() {
	request := &client.BulkableRequest{}
	key := "test-key"
	duplicates := 100
	ackChs := make([]<-chan bool, duplicates)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any()).Times(duplicates - 1)

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
			s.False(ack)
		default:
			pendingRequestsCount++
		}
	}

	s.Equal(1, pendingRequestsCount, "only one request should not be nacked")
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

	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
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

	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
	mapVal := newAckChan()
	s.esProcessor.mapToAckChan.Put(testKey, mapVal)
	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorFailures)
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

	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorFailures)
	s.esProcessor.bulkAfterAction(0, requests, response, errors.New("some error"))
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

	s.mockMetricClient.EXPECT().AddCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorRequests, int64(1))
	s.mockMetricClient.EXPECT().RecordDistribution(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorBulkSize, 1)
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorWaitLatency, gomock.Any())

	mapVal := newAckChan()
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
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
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
	s.mockMetricClient.EXPECT().RecordTimer(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorRequestLatency, gomock.Any())
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
	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorCorruptedData)

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

	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticsearchVisibility, metrics.ElasticsearchBulkProcessorCorruptedData)
	key := s.esProcessor.extractVisibilityTaskKey(request)
	s.Equal("", key)

	id := "id"
	request.Id(id)
	key = s.esProcessor.extractVisibilityTaskKey(request)
	s.Equal(id, key)
}

func (s *processorSuite) TestIsResponseSuccess() {
	for i := 200; i < 300; i++ {
		s.True(isSuccessStatus(i))
	}
	status := []int{409, 404}
	for _, code := range status {
		s.True(isSuccessStatus(code))
	}
	status = []int{100, 199, 300, 400, 500, 408, 429, 503, 507}
	for _, code := range status {
		s.False(isSuccessStatus(code))
	}
}

func (s *processorSuite) TestErrorReasonFromResponse() {
	reason := "error reason"
	resp := &elastic.BulkResponseItem{Status: 400}
	s.Equal("", extractErrorReason(resp))
	resp.Error = &elastic.ErrorDetails{Reason: reason}
	s.Equal(reason, extractErrorReason(resp))
}
