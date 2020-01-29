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
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/olivere/elastic"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/uber/cadence/common/collection"
	es "github.com/uber/cadence/common/elasticsearch"
	esMocks "github.com/uber/cadence/common/elasticsearch/mocks"
	"github.com/uber/cadence/common/log/loggerimpl"
	msgMocks "github.com/uber/cadence/common/messaging/mocks"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/worker/indexer/mocks"
)

type esProcessorSuite struct {
	suite.Suite
	esProcessor       *esProcessorImpl
	mockBulkProcessor *mocks.ElasticBulkProcessor
	mockMetricClient  *mmocks.Client
	mockESClient      *esMocks.Client
}

var (
	testIndex     = "test-index"
	testType      = esDocType
	testID        = "test-doc-id"
	testStopWatch = metrics.NopStopwatch()
	testScope     = metrics.ESProcessorScope
	testMetric    = metrics.ESProcessorProcessMsgLatency
)

func TestESProcessorSuite(t *testing.T) {
	s := new(esProcessorSuite)
	suite.Run(t, s)
}

func (s *esProcessorSuite) SetupSuite() {
}

func (s *esProcessorSuite) SetupTest() {
	config := &Config{
		IndexerConcurrency:       dynamicconfig.GetIntPropertyFn(32),
		ESProcessorNumOfWorkers:  dynamicconfig.GetIntPropertyFn(1),
		ESProcessorBulkActions:   dynamicconfig.GetIntPropertyFn(10),
		ESProcessorBulkSize:      dynamicconfig.GetIntPropertyFn(2 << 20),
		ESProcessorFlushInterval: dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
	}
	s.mockMetricClient = &mmocks.Client{}
	s.mockBulkProcessor = &mocks.ElasticBulkProcessor{}

	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)

	p := &esProcessorImpl{
		config:        config,
		logger:        loggerimpl.NewLogger(zapLogger),
		metricsClient: s.mockMetricClient,
	}
	p.mapToKafkaMsg = collection.NewShardedConcurrentTxMap(1024, p.hashFn)
	p.processor = s.mockBulkProcessor

	s.esProcessor = p

	s.mockESClient = &esMocks.Client{}
}

func (s *esProcessorSuite) TearDownTest() {
	s.mockBulkProcessor.AssertExpectations(s.T())
	s.mockMetricClient.AssertExpectations(s.T())
	s.mockESClient.AssertExpectations(s.T())
}

func (s *esProcessorSuite) TestNewESProcessorAndStart() {
	config := &Config{
		ESProcessorNumOfWorkers:  dynamicconfig.GetIntPropertyFn(1),
		ESProcessorBulkActions:   dynamicconfig.GetIntPropertyFn(10),
		ESProcessorBulkSize:      dynamicconfig.GetIntPropertyFn(2 << 20),
		ESProcessorFlushInterval: dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
	}
	processorName := "test-processor"

	s.mockESClient.On("RunBulkProcessor", mock.Anything, mock.MatchedBy(func(input *es.BulkProcessorParameters) bool {
		s.Equal(processorName, input.Name)
		s.Equal(config.ESProcessorNumOfWorkers(), input.NumOfWorkers)
		s.Equal(config.ESProcessorBulkActions(), input.BulkActions)
		s.Equal(config.ESProcessorBulkSize(), input.BulkSize)
		s.Equal(config.ESProcessorFlushInterval(), input.FlushInterval)
		s.NotNil(input.Backoff)
		s.NotNil(input.AfterFunc)
		return true
	})).Return(&elastic.BulkProcessor{}, nil).Once()
	p, err := NewESProcessorAndStart(config, s.mockESClient, processorName, s.esProcessor.logger, &mmocks.Client{})
	s.NoError(err)

	processor, ok := p.(*esProcessorImpl)
	s.True(ok)
	s.NotNil(processor.mapToKafkaMsg)

	p.Stop()
}

func (s *esProcessorSuite) TestStop() {
	s.mockBulkProcessor.On("Stop").Return(nil).Once()
	s.esProcessor.Stop()
	s.Nil(s.esProcessor.mapToKafkaMsg)
}

func (s *esProcessorSuite) TestAdd() {
	request := elastic.NewBulkIndexRequest()
	mockKafkaMsg := &msgMocks.Message{}
	key := "test-key"
	s.Equal(0, s.esProcessor.mapToKafkaMsg.Len())

	s.mockBulkProcessor.On("Add", request).Return().Once()
	s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Once()
	s.esProcessor.Add(request, key, mockKafkaMsg)
	s.Equal(1, s.esProcessor.mapToKafkaMsg.Len())
	mockKafkaMsg.AssertExpectations(s.T())

	// handle duplicate
	mockKafkaMsg.On("Ack").Return(nil).Once()
	s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Once()
	s.esProcessor.Add(request, key, mockKafkaMsg)
	s.Equal(1, s.esProcessor.mapToKafkaMsg.Len())
	mockKafkaMsg.AssertExpectations(s.T())
}

func (s *esProcessorSuite) TestAdd_ConcurrentAdd() {
	request := elastic.NewBulkIndexRequest()
	mockKafkaMsg := &msgMocks.Message{}
	key := "test-key"

	addFunc := func(wg *sync.WaitGroup) {
		s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Once()
		s.esProcessor.Add(request, key, mockKafkaMsg)
		wg.Done()
	}
	duplicates := 5
	wg := &sync.WaitGroup{}
	wg.Add(duplicates)
	s.mockBulkProcessor.On("Add", request).Return().Once()
	mockKafkaMsg.On("Ack").Return(nil).Times(duplicates - 1)
	for i := 0; i < duplicates; i++ {
		addFunc(wg)
	}
	wg.Wait()
	mockKafkaMsg.AssertExpectations(s.T())
}

func (s *esProcessorSuite) TestBulkAfterActionX() {
	version := int64(3)
	testKey := "testKey"
	request := elastic.NewBulkIndexRequest().
		Index(testIndex).
		Type(testType).
		Id(testID).
		VersionType(versionTypeExternal).
		Version(version).
		Doc(map[string]interface{}{es.KafkaKey: testKey})
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

	mockKafkaMsg := &msgMocks.Message{}
	mapVal := newKafkaMessageWithMetrics(mockKafkaMsg, &testStopWatch)
	s.esProcessor.mapToKafkaMsg.Put(testKey, mapVal)
	mockKafkaMsg.On("Ack").Return(nil).Once()
	s.esProcessor.bulkAfterAction(0, requests, response, nil)
	mockKafkaMsg.AssertExpectations(s.T())
}

func (s *esProcessorSuite) TestBulkAfterAction_Nack() {
	version := int64(3)
	testKey := "testKey"
	request := elastic.NewBulkIndexRequest().
		Index(testIndex).
		Type(testType).
		Id(testID).
		VersionType(versionTypeExternal).
		Version(version).
		Doc(map[string]interface{}{es.KafkaKey: testKey})
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

	mockKafkaMsg := &msgMocks.Message{}
	mapVal := newKafkaMessageWithMetrics(mockKafkaMsg, &testStopWatch)
	s.esProcessor.mapToKafkaMsg.Put(testKey, mapVal)
	mockKafkaMsg.On("Nack").Return(nil).Once()
	s.esProcessor.bulkAfterAction(0, requests, response, nil)
	mockKafkaMsg.AssertExpectations(s.T())
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

	s.mockMetricClient.On("IncCounter", metrics.ESProcessorScope, metrics.ESProcessorFailures).Once()
	s.esProcessor.bulkAfterAction(0, requests, response, errors.New("some error"))
}

func (s *esProcessorSuite) TestAckKafkaMsg() {
	key := "test-key"
	// no msg in map, nothing called
	s.esProcessor.ackKafkaMsg(key)

	request := elastic.NewBulkIndexRequest()
	mockKafkaMsg := &msgMocks.Message{}
	s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Once()
	s.mockBulkProcessor.On("Add", request).Return().Once()
	s.esProcessor.Add(request, key, mockKafkaMsg)
	s.Equal(1, s.esProcessor.mapToKafkaMsg.Len())

	mockKafkaMsg.On("Ack").Return(nil).Once()
	s.esProcessor.ackKafkaMsg(key)
	mockKafkaMsg.AssertExpectations(s.T())
	s.Equal(0, s.esProcessor.mapToKafkaMsg.Len())
}

func (s *esProcessorSuite) TestNackKafkaMsg() {
	key := "test-key-nack"
	// no msg in map, nothing called
	s.esProcessor.nackKafkaMsg(key)

	request := elastic.NewBulkIndexRequest()
	mockKafkaMsg := &msgMocks.Message{}
	s.mockBulkProcessor.On("Add", request).Return().Once()
	s.mockMetricClient.On("StartTimer", testScope, testMetric).Return(testStopWatch).Once()
	s.esProcessor.Add(request, key, mockKafkaMsg)
	s.Equal(1, s.esProcessor.mapToKafkaMsg.Len())

	mockKafkaMsg.On("Nack").Return(nil).Once()
	s.esProcessor.nackKafkaMsg(key)
	mockKafkaMsg.AssertExpectations(s.T())
	s.Equal(0, s.esProcessor.mapToKafkaMsg.Len())
}

func (s *esProcessorSuite) TestHashFn() {
	s.Equal(uint32(0), s.esProcessor.hashFn(0))
	s.NotEqual(uint32(0), s.esProcessor.hashFn("test"))
}

func (s *esProcessorSuite) TestGetKeyForKafkaMsg() {
	request := elastic.NewBulkIndexRequest()
	s.PanicsWithValue("KafkaKey not found", func() { s.esProcessor.getKeyForKafkaMsg(request) })

	m := map[string]interface{}{
		es.KafkaKey: 1,
	}
	request.Doc(m)
	s.PanicsWithValue("KafkaKey is not string", func() { s.esProcessor.getKeyForKafkaMsg(request) })

	testKey := "test-key"
	m[es.KafkaKey] = testKey
	request.Doc(m)
	s.Equal(testKey, s.esProcessor.getKeyForKafkaMsg(request))
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

	s.PanicsWithValue("_id not found in request opMap", func() { s.esProcessor.getKeyForKafkaMsg(request) })

	id := "id"
	request.Id(id)
	key := s.esProcessor.getKeyForKafkaMsg(request)
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

func (s *esProcessorSuite) TestIsResponseRetriable() {
	status := []int{408, 429, 500, 503, 507}
	for _, code := range status {
		s.True(isResponseRetriable(code))
	}
}

func (s *esProcessorSuite) TestGetErrorMsgFromESResp() {
	reason := "error reason"
	resp := &elastic.BulkResponseItem{Status: 400}
	s.Equal("", getErrorMsgFromESResp(resp))
	resp.Error = &elastic.ErrorDetails{Reason: reason}
	s.Equal(reason, getErrorMsgFromESResp(resp))
}
