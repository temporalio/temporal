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
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	es "go.temporal.io/server/common/elasticsearch"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/metrics"
	metricsmocks "go.temporal.io/server/common/metrics/mocks"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type esProcessorSuite struct {
	suite.Suite
	controller        *gomock.Controller
	esProcessor       *esProcessorImpl
	mockBulkProcessor *MockElasticBulkProcessor
	mockMetricClient  *metricsmocks.Client
	mockESClient      *es.MockClient
}

var (
	testType      = docType
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
	s.mockMetricClient = &metricsmocks.Client{}

	s.mockBulkProcessor = NewMockElasticBulkProcessor(s.controller)
	s.mockESClient = es.NewMockClient(s.controller)
	s.esProcessor = NewProcessor(cfg, s.mockESClient, loggerimpl.NewLogger(zapLogger), s.mockMetricClient)

	// esProcessor.Start mock
	s.esProcessor.mapToAckChan = collection.NewShardedConcurrentTxMap(1024, s.esProcessor.hashFn)
	s.esProcessor.bulkProcessor = s.mockBulkProcessor
}

func (s *esProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockMetricClient.AssertExpectations(s.T())
}

func (s *esProcessorSuite) TestNewESProcessorAndStartStop() {
	config := &ProcessorConfig{
		IndexerConcurrency:       dynamicconfig.GetIntPropertyFn(32),
		ESProcessorNumOfWorkers:  dynamicconfig.GetIntPropertyFn(1),
		ESProcessorBulkActions:   dynamicconfig.GetIntPropertyFn(10),
		ESProcessorBulkSize:      dynamicconfig.GetIntPropertyFn(2 << 20),
		ESProcessorFlushInterval: dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
	}

	p := NewProcessor(config, s.mockESClient, s.esProcessor.logger, &metricsmocks.Client{})

	s.mockESClient.EXPECT().RunBulkProcessor(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *es.BulkProcessorParameters) (*elastic.BulkProcessor, error) {
			s.Equal(visibilityProcessorName, input.Name)
			s.Equal(config.ESProcessorNumOfWorkers(), input.NumOfWorkers)
			s.Equal(config.ESProcessorBulkActions(), input.BulkActions)
			s.Equal(config.ESProcessorBulkSize(), input.BulkSize)
			s.Equal(config.ESProcessorFlushInterval(), input.FlushInterval)
			s.NotNil(input.Backoff)
			s.NotNil(input.AfterFunc)
			return &elastic.BulkProcessor{}, nil
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
	request := elastic.NewBulkIndexRequest()
	visibilityTaskKey := "test-key"
	ackCh := make(chan bool, 1)
	s.Equal(0, s.esProcessor.mapToAckChan.Len())

	s.mockBulkProcessor.EXPECT().Add(request).Times(1)
	s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Once()

	s.esProcessor.Add(request, visibilityTaskKey, ackCh)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())
	select {
	case <-ackCh:
		s.Fail("request shouldn't be acknowledged")
	default:
	}

	// handle duplicate
	s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Once()
	s.esProcessor.Add(request, visibilityTaskKey, ackCh)
	s.Equal(1, s.esProcessor.mapToAckChan.Len())
	select {
	case ack := <-ackCh:
		s.True(ack)
	default:
		s.Fail("request should be acknowledged due to duplicate key")
	}
}

func (s *esProcessorSuite) TestAdd_ConcurrentAdd() {
	request := elastic.NewBulkIndexRequest()
	key := "test-key"
	duplicates := 100
	ackCh := make(chan bool, duplicates-1)
	s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Times(duplicates)

	addFunc := func(wg *sync.WaitGroup) {
		s.esProcessor.Add(request, key, ackCh)
		wg.Done()
	}
	wg := &sync.WaitGroup{}
	wg.Add(duplicates)
	s.mockBulkProcessor.EXPECT().Add(request).Times(1)
	for i := 0; i < duplicates; i++ {
		go addFunc(wg)
	}
	wg.Wait()
	for i := 0; i < duplicates-1; i++ {
		ack := <-ackCh
		s.True(ack)
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
		Type(testType).
		Id(testID).
		VersionType(versionTypeExternal).
		Version(version).
		Doc(map[string]interface{}{definition.VisibilityTaskKey: testKey})
	requests := []elastic.BulkableRequest{request}

	mSuccess := map[string]*elastic.BulkResponseItem{
		"index": {
			Index:   testIndex,
			Type:    testType,
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
		Type(testType).
		Id(testID).
		VersionType(versionTypeExternal).
		Version(version).
		Doc(map[string]interface{}{
			definition.VisibilityTaskKey: testKey,
			definition.NamespaceID:       namespaceID,
			definition.WorkflowID:        wid,
			definition.RunID:             rid,
		})
	requests := []elastic.BulkableRequest{request}

	mFailed := map[string]*elastic.BulkResponseItem{
		"index": {
			Index:   testIndex,
			Type:    testType,
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
	request := elastic.NewBulkIndexRequest().
		Index(testIndex).
		Type(testType).
		Id(testID).
		VersionType(versionTypeExternal).
		Version(version)
	requests := []elastic.BulkableRequest{request}

	mFailed := map[string]*elastic.BulkResponseItem{
		"index": {
			Index:   testIndex,
			Type:    testType,
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

	s.mockMetricClient.On("IncCounter", metrics.ElasticSearchVisibility, metrics.ESBulkProcessorFailures).Once()
	s.esProcessor.bulkAfterAction(0, requests, response, errors.New("some error"))
}

func (s *esProcessorSuite) TestAckChan() {
	key := "test-key"
	// no msg in map, nothing called
	s.esProcessor.sendToAckChan(key, true)

	request := elastic.NewBulkIndexRequest()
	s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Once()
	s.mockBulkProcessor.EXPECT().Add(request).Times(1)
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

	request := elastic.NewBulkIndexRequest()
	s.mockBulkProcessor.EXPECT().Add(request).Times(1)
	s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Once()
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

// func (s *esProcessorSuite) getEncodedMsg(wid string, rid string, namespaceID string) []byte {
// 	indexMsg := &indexerspb.Message{
// 		NamespaceId: namespaceID,
// 		WorkflowId:  wid,
// 		RunId:       rid,
// 	}
// 	payload, err := s.esProcessor.msgEncoder.Encode(indexMsg)
// 	s.NoError(err)
// 	return payload
// }
//
func (s *esProcessorSuite) TestGetDocIDs() {
	testKey := "test-key"
	testWid := "test-workflowID"
	testRid := "test-runID"
	testNamespaceid := "test-namespaceID"

	request := elastic.NewBulkIndexRequest().
		Doc(map[string]interface{}{
			definition.VisibilityTaskKey: testKey,
			definition.NamespaceID:       testNamespaceid,
			definition.WorkflowID:        testWid,
			definition.RunID:             testRid,
		})

	wid, rid, namespaceID := s.esProcessor.getDocIDs(request)
	s.Equal(testWid, wid)
	s.Equal(testRid, rid)
	s.Equal(testNamespaceid, namespaceID)
}

func (s *esProcessorSuite) TestGetDocIDs_Error() {
	testKey := "test-key"
	request := elastic.NewBulkIndexRequest().
		Doc(map[string]interface{}{
			definition.VisibilityTaskKey: testKey,
		})
	wid, rid, namespaceID := s.esProcessor.getDocIDs(request)
	s.Equal("", wid)
	s.Equal("", rid)
	s.Equal("", namespaceID)
}

func (s *esProcessorSuite) TestGetVisibilityTaskKey() {
	request := elastic.NewBulkIndexRequest()
	s.PanicsWithValue("VisibilityTaskKey not found", func() { s.esProcessor.getVisibilityTaskKey(request) })

	m := map[string]interface{}{
		definition.VisibilityTaskKey: 1,
	}
	request.Doc(m)
	s.PanicsWithValue("VisibilityTaskKey is not string", func() { s.esProcessor.getVisibilityTaskKey(request) })

	testKey := "test-key"
	m[definition.VisibilityTaskKey] = testKey
	request.Doc(m)
	s.Equal(testKey, s.esProcessor.getVisibilityTaskKey(request))
}

func (s *esProcessorSuite) TestGetKeyForKafkaMsg_Delete() {
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

	s.PanicsWithValue("_id not found in request opMap", func() { s.esProcessor.getVisibilityTaskKey(request) })

	id := "id"
	request.Id(id)
	key := s.esProcessor.getVisibilityTaskKey(request)
	s.Equal(id, key)
}

func (s *esProcessorSuite) TestIsResponseSuccess() {
	for i := 200; i < 300; i++ {
		s.True(isResponseSuccess(i))
	}
	status := []int{409, 404}
	for _, code := range status {
		s.True(isResponseSuccess(code))
	}
	status = []int{100, 199, 300, 400, 500, 408, 429, 503, 507}
	for _, code := range status {
		s.False(isResponseSuccess(code))
	}
}

func (s *esProcessorSuite) TestIsResponseRetryable() {
	status := []int{408, 429, 500, 503, 507}
	for _, code := range status {
		s.True(isResponseRetryable(code))
	}
}

func (s *esProcessorSuite) TestGetErrorMsgFromESResp() {
	reason := "error reason"
	resp := &elastic.BulkResponseItem{Status: 400}
	s.Equal("", getErrorMsgFromESResp(resp))
	resp.Error = &elastic.ErrorDetails{Reason: reason}
	s.Equal(reason, getErrorMsgFromESResp(resp))
}
