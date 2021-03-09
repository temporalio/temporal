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
	"go.uber.org/zap"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type esProcessorSuite struct {
	suite.Suite
	controller        *gomock.Controller
	esProcessor       *esProcessorImpl
	mockBulkProcessor *MockBulkProcessor
	mockMetricClient  *metrics.MockClient
	mockESClient      *MockClient
}

var (
	testID        = "test-doc-id"
	testStopWatch = metrics.NopStopwatch()
	testScope     = metrics.ElasticSearchVisibility
	testMetric    = metrics.ESBulkProcessorRequestLatency
)

func TestESProcessorSuite(t *testing.T) {
	s := new(esProcessorSuite)
	suite.Run(t, s)
}

func (s *esProcessorSuite) SetupSuite() {
}

func (s *esProcessorSuite) SetupTest() {
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)

	s.controller = gomock.NewController(s.T())

	cfg := &ProcessorConfig{
		IndexerConcurrency:       dynamicconfig.GetIntPropertyFn(32),
		ESProcessorNumOfWorkers:  dynamicconfig.GetIntPropertyFn(1),
		ESProcessorBulkActions:   dynamicconfig.GetIntPropertyFn(10),
		ESProcessorBulkSize:      dynamicconfig.GetIntPropertyFn(2 << 20),
		ESProcessorFlushInterval: dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
	}

	s.mockMetricClient = metrics.NewMockClient(s.controller)
	s.mockBulkProcessor = NewMockBulkProcessor(s.controller)
	s.mockESClient = NewMockClient(s.controller)
	s.esProcessor = NewProcessor(cfg, s.mockESClient, loggerimpl.NewLogger(zapLogger), s.mockMetricClient)

	// esProcessor.Start mock
	s.esProcessor.mapToAckChan = collection.NewShardedConcurrentTxMap(1024, s.esProcessor.hashFn)
	s.esProcessor.bulkProcessor = s.mockBulkProcessor
}

func (s *esProcessorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *esProcessorSuite) TestNewESProcessorAndStartStop() {
	config := &ProcessorConfig{
		IndexerConcurrency:       dynamicconfig.GetIntPropertyFn(32),
		ESProcessorNumOfWorkers:  dynamicconfig.GetIntPropertyFn(1),
		ESProcessorBulkActions:   dynamicconfig.GetIntPropertyFn(10),
		ESProcessorBulkSize:      dynamicconfig.GetIntPropertyFn(2 << 20),
		ESProcessorFlushInterval: dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
	}

	p := NewProcessor(config, s.mockESClient, s.esProcessor.logger, s.mockMetricClient)

	s.mockESClient.EXPECT().RunBulkProcessor(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *BulkProcessorParameters) (BulkProcessor, error) {
			s.Equal(visibilityProcessorName, input.Name)
			s.Equal(config.ESProcessorNumOfWorkers(), input.NumOfWorkers)
			s.Equal(config.ESProcessorBulkActions(), input.BulkActions)
			s.Equal(config.ESProcessorBulkSize(), input.BulkSize)
			s.Equal(config.ESProcessorFlushInterval(), input.FlushInterval)
			s.NotNil(input.Backoff)
			s.NotNil(input.AfterFunc)

			bulkProcessor := NewMockBulkProcessor(s.controller)
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

func (s *esProcessorSuite) TestAdd() {
	request := &BulkableRequest{}
	visibilityTaskKey := "test-key"
	ackCh := make(chan bool, 1)
	s.Equal(0, s.esProcessor.mapToAckChan.Len())

	s.mockBulkProcessor.EXPECT().Add(request)
	s.mockMetricClient.EXPECT().StartTimer(testScope, testMetric).Return(testStopWatch)

	s.esProcessor.Add(request, visibilityTaskKey, ackCh)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())
	select {
	case <-ackCh:
		s.Fail("request shouldn't be acknowledged")
	default:
	}

	// handle duplicate
	s.mockMetricClient.EXPECT().StartTimer(testScope, testMetric).Return(testStopWatch)
	s.esProcessor.Add(request, visibilityTaskKey, ackCh)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())
	select {
	case ack := <-ackCh:
		s.False(ack)
	default:
		s.Fail("request should be nacked due to duplicate key")
	}
}

func (s *esProcessorSuite) TestAdd_ConcurrentAdd() {
	request := &BulkableRequest{}
	key := "test-key"
	duplicates := 100
	ackCh := make(chan bool, duplicates-1)
	s.mockMetricClient.EXPECT().StartTimer(testScope, testMetric).Return(testStopWatch).Times(duplicates)

	addFunc := func(wg *sync.WaitGroup) {
		s.esProcessor.Add(request, key, ackCh)
		wg.Done()
	}
	wg := &sync.WaitGroup{}
	wg.Add(duplicates)
	s.mockBulkProcessor.EXPECT().Add(request)
	for i := 0; i < duplicates; i++ {
		go addFunc(wg)
	}
	wg.Wait()
	for i := 0; i < duplicates-1; i++ {
		ack := <-ackCh
		s.False(ack)
	}
	select {
	case <-ackCh:
		s.Fail("there should be no more acknowledged requests")
	default:
	}
}

func (s *esProcessorSuite) TestBulkAfterAction_Ack() {
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

	ackCh := make(chan bool, 1)
	mapVal := newAckChanWithStopwatch(ackCh, &testStopWatch)
	s.esProcessor.mapToAckChan.Put(testKey, mapVal)
	s.esProcessor.bulkAfterAction(0, requests, response, nil)
	select {
	case ack := <-ackCh:
		s.True(ack)
	default:
		s.Fail("request should be acknowledged")
	}
}

func (s *esProcessorSuite) TestBulkAfterAction_Nack() {
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

	ackCh := make(chan bool, 1)
	mapVal := newAckChanWithStopwatch(ackCh, &testStopWatch)
	s.esProcessor.mapToAckChan.Put(testKey, mapVal)
	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorFailures)
	s.esProcessor.bulkAfterAction(0, requests, response, nil)
	select {
	case ack := <-ackCh:
		s.False(ack)
	default:
		s.Fail("request should be acknowledged")
	}
}

func (s *esProcessorSuite) TestBulkAfterAction_Error() {
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

	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorFailures)
	s.esProcessor.bulkAfterAction(0, requests, response, errors.New("some error"))
}

func (s *esProcessorSuite) TestAckChan() {
	key := "test-key"
	// no msg in map, nothing called
	s.esProcessor.sendToAckChan(key, true)

	request := &BulkableRequest{}
	s.mockMetricClient.EXPECT().StartTimer(testScope, testMetric).Return(testStopWatch)
	s.mockBulkProcessor.EXPECT().Add(request)
	ackCh := make(chan bool, 1)
	s.esProcessor.Add(request, key, ackCh)
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

func (s *esProcessorSuite) TestNackChan() {
	key := "test-key-nack"
	// no msg in map, nothing called
	s.esProcessor.sendToAckChan(key, false)

	request := &BulkableRequest{}
	s.mockBulkProcessor.EXPECT().Add(request)
	s.mockMetricClient.EXPECT().StartTimer(testScope, testMetric).Return(testStopWatch)
	ackCh := make(chan bool, 1)
	s.esProcessor.Add(request, key, ackCh)
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

func (s *esProcessorSuite) TestHashFn() {
	s.Equal(uint32(0), s.esProcessor.hashFn(0))
	s.NotEqual(uint32(0), s.esProcessor.hashFn("test"))
}

func (s *esProcessorSuite) TestExtractVisibilityTaskKey() {
	request := elastic.NewBulkIndexRequest()
	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorCorruptedData)

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

func (s *esProcessorSuite) TestExtractVisibilityTaskKey_Delete() {
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

	s.mockMetricClient.EXPECT().IncCounter(metrics.ElasticSearchVisibility, metrics.ESBulkProcessorCorruptedData)
	key := s.esProcessor.extractVisibilityTaskKey(request)
	s.Equal("", key)

	id := "id"
	request.Id(id)
	key = s.esProcessor.extractVisibilityTaskKey(request)
	s.Equal(id, key)
}

func (s *esProcessorSuite) TestIsResponseSuccess() {
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

func (s *esProcessorSuite) TestIsResponseRetryable() {
	status := []int{408, 429, 500, 503, 507}
	for _, code := range status {
		s.True(isRetryableStatus(code))
	}
}

func (s *esProcessorSuite) TestErrorReasonFromResponse() {
	reason := "error reason"
	resp := &elastic.BulkResponseItem{Status: 400}
	s.Equal("", extractErrorReason(resp))
	resp.Error = &elastic.ErrorDetails{Reason: reason}
	s.Equal(reason, extractErrorReason(resp))
}
