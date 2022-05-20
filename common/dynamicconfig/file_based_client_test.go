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

package dynamicconfig

import (
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
)

type fileBasedClientSuite struct {
	suite.Suite
	*require.Assertions
	client Client
	doneCh chan interface{}
}

func TestFileBasedClientSuite(t *testing.T) {
	s := new(fileBasedClientSuite)
	suite.Run(t, s)
}

func (s *fileBasedClientSuite) SetupSuite() {
	var err error
	s.doneCh = make(chan interface{})
	s.client, err = NewFileBasedClient(&FileBasedClientConfig{
		Filepath:     "config/testConfig.yaml",
		PollInterval: time.Second * 5,
	}, log.NewNoopLogger(), s.doneCh)
	s.Require().NoError(err)
}

func (s *fileBasedClientSuite) TearDownSuite() {
	close(s.doneCh)
}

func (s *fileBasedClientSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *fileBasedClientSuite) TestGetValue() {
	v, err := s.client.GetValue(testGetBoolPropertyKey, true)
	s.NoError(err)
	s.Equal(false, v)
}

func (s *fileBasedClientSuite) TestGetValue_NonExistKey() {
	defaultValue := true
	v, err := s.client.GetValue(unknownKey, defaultValue)
	s.Error(err)
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetValue_CaseInsensitie() {
	v, err := s.client.GetValue(testCaseInsensitivePropertyKey, false)
	s.Nil(err)
	s.Equal(true, v)
}

func (s *fileBasedClientSuite) TestGetValueWithFilters() {
	filters := []map[Filter]interface{}{{
		Namespace: "global-samples-namespace",
	}}
	v, err := s.client.GetValueWithFilters(testGetBoolPropertyKey, filters, false)
	s.NoError(err)
	s.Equal(true, v)

	filters = []map[Filter]interface{}{{
		Namespace: "non-exist-namespace",
	}}
	v, err = s.client.GetValueWithFilters(testGetBoolPropertyKey, filters, true)
	s.NoError(err)
	s.Equal(false, v)

	filters = []map[Filter]interface{}{{
		Namespace:     "samples-namespace",
		TaskQueueName: "non-exist-taskqueue",
	}}
	v, err = s.client.GetValueWithFilters(testGetBoolPropertyKey, filters, false)
	s.NoError(err)
	s.Equal(false, v)
}

func (s *fileBasedClientSuite) TestGetValueWithFilters_UnknownFilter() {
	filters := []map[Filter]interface{}{{
		Namespace:     "global-samples-namespace",
		unknownFilter: "unknown-filter",
	}}
	v, err := s.client.GetValueWithFilters(testGetBoolPropertyKey, filters, false)
	s.NoError(err)
	s.Equal(false, v)
}

func (s *fileBasedClientSuite) TestGetIntValue() {
	v, err := s.client.GetIntValue(testGetIntPropertyKey, nil, 1)
	s.NoError(err)
	s.Equal(1000, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterNotMatch() {
	filters := []map[Filter]interface{}{{
		Namespace: "samples-namespace",
	}}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, 500)
	s.NoError(err)
	s.Equal(1000, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_WrongType() {
	defaultValue := 2000
	filters := []map[Filter]interface{}{{
		Namespace: "global-samples-namespace",
	}}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, defaultValue)
	s.Error(err)
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByWorkflowTaskQueueInfo() {
	expectedValue := 1001
	filters := []map[Filter]interface{}{{
		Namespace:     "global-samples-namespace",
		TaskQueueName: "test-tq",
		TaskType:      "Workflow",
	}}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, 0)
	s.NoError(err)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByNoTaskTypeQueueInfo() {
	expectedValue := 1003
	filters := []map[Filter]interface{}{{
		Namespace:     "global-samples-namespace",
		TaskQueueName: "test-tq",
	}}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, 0)
	s.NoError(err)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByActivityTaskQueueInfo() {
	expectedValue := 1002
	filters := []map[Filter]interface{}{{
		Namespace:     "global-samples-namespace",
		TaskQueueName: "test-tq",
		TaskType:      "Activity",
	}}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, 0)
	s.NoError(err)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByTaskQueueNameOnly() {
	expectedValue := 1005
	filters := []map[Filter]interface{}{{
		TaskQueueName: "other-test-tq",
	}}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, 0)
	s.NoError(err)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterByTQ_MatchWithoutType() {
	cln := NewCollection(s.client, log.NewNoopLogger())
	expectedValue := 1003
	f := cln.GetIntPropertyFilteredByTaskQueueInfo(testGetIntPropertyKey, 0)
	// 5 is nonsense value that doesn't match either Workflow or Activity
	v := f("global-samples-namespace", "test-tq", 5)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterByTQ_NamespaceOnly() {
	cln := NewCollection(s.client, log.NewNoopLogger())
	expectedValue := 1004
	f := cln.GetIntPropertyFilteredByTaskQueueInfo(testGetIntPropertyKey, 0)
	v := f("another-namespace", "other-test-tq", 0)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterByTQ_MatchFallback() {
	cln := NewCollection(s.client, log.NewNoopLogger())
	// should return 1001 as the most specific match
	f1 := cln.GetIntPropertyFilteredByTaskQueueInfo(testGetIntPropertyKey, 1001)
	v1 := f1("global-samples-namespace", "test-tq", 1)
	f2 := cln.GetIntPropertyFilteredByTaskQueueInfo(testGetIntPropertyKey, 0)
	v2 := f2("global-samples-namespace", "test-tq", 1)
	s.Equal(v1, v2)
}

func (s *fileBasedClientSuite) TestGetFloatValue() {
	v, err := s.client.GetFloatValue(testGetFloat64PropertyKey, nil, 1)
	s.NoError(err)
	s.Equal(12.0, v)
}

func (s *fileBasedClientSuite) TestGetFloatValue_WrongType() {
	filters := []map[Filter]interface{}{{
		Namespace: "samples-namespace",
	}}
	defaultValue := 1.0
	v, err := s.client.GetFloatValue(testGetFloat64PropertyKey, filters, defaultValue)
	s.Error(err)
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetBoolValue() {
	v, err := s.client.GetBoolValue(testGetBoolPropertyKey, nil, true)
	s.NoError(err)
	s.Equal(false, v)
}

func (s *fileBasedClientSuite) TestGetStringValue() {
	filters := []map[Filter]interface{}{{
		TaskQueueName: "random taskqueue",
	}}
	v, err := s.client.GetStringValue(testGetStringPropertyKey, filters, "defaultString")
	s.NoError(err)
	s.Equal("constrained-string", v)
}

func (s *fileBasedClientSuite) TestGetMapValue() {
	var defaultVal map[string]interface{}
	v, err := s.client.GetMapValue(testGetMapPropertyKey, nil, defaultVal)
	s.NoError(err)
	expectedVal := map[string]interface{}{
		"key1": "1",
		"key2": 1,
		"key3": []interface{}{
			false,
			map[string]interface{}{
				"key4": true,
				"key5": 2.1,
			},
		},
	}
	s.Equal(expectedVal, v)
}

func (s *fileBasedClientSuite) TestGetMapValue_WrongType() {
	var defaultVal map[string]interface{}
	filters := []map[Filter]interface{}{{
		TaskQueueName: "random taskqueue",
	}}
	v, err := s.client.GetMapValue(testGetMapPropertyKey, filters, defaultVal)
	s.Error(err)
	s.Equal(defaultVal, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue() {
	v, err := s.client.GetDurationValue(testGetDurationPropertyKey, nil, time.Second)
	s.NoError(err)
	s.Equal(time.Minute, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_NotStringRepresentation() {
	filters := []map[Filter]interface{}{{
		Namespace: "samples-namespace",
	}}
	v, err := s.client.GetDurationValue(testGetDurationPropertyKey, filters, time.Second)
	s.Error(err)
	s.Equal(time.Second, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_ParseFailed() {
	filters := []map[Filter]interface{}{{
		Namespace:     "samples-namespace",
		TaskQueueName: "longIdleTimeTaskqueue",
	}}
	v, err := s.client.GetDurationValue(testGetDurationPropertyKey, filters, time.Second)
	s.Error(err)
	s.Equal(time.Second, v)
}

func (s *fileBasedClientSuite) TestValidateConfig_ConfigNotExist() {
	_, err := NewFileBasedClient(nil, nil, nil)
	s.Error(err)
}

func (s *fileBasedClientSuite) TestValidateConfig_FileNotExist() {
	_, err := NewFileBasedClient(&FileBasedClientConfig{
		Filepath:     "file/not/exist.yaml",
		PollInterval: time.Second * 10,
	}, nil, nil)
	s.Error(err)
}

func (s *fileBasedClientSuite) TestValidateConfig_ShortPollInterval() {
	_, err := NewFileBasedClient(&FileBasedClientConfig{
		Filepath:     "config/testConfig.yaml",
		PollInterval: time.Second,
	}, nil, nil)
	s.Error(err)
}

func (s *fileBasedClientSuite) TestMatch() {
	testCases := []struct {
		v       *constrainedValue
		filters map[Filter]interface{}
		matched bool
	}{
		{
			v: &constrainedValue{
				Constraints: map[string]interface{}{},
			},
			filters: map[Filter]interface{}{
				Namespace: "some random namespace",
			},
			matched: false,
		},
		{
			v: &constrainedValue{
				Constraints: map[string]interface{}{"some key": "some value"},
			},
			filters: map[Filter]interface{}{},
			matched: false,
		},
		{
			v: &constrainedValue{
				Constraints: map[string]interface{}{"namespace": "samples-namespace"},
			},
			filters: map[Filter]interface{}{
				Namespace: "some random namespace",
			},
			matched: false,
		},
		{
			v: &constrainedValue{
				Constraints: map[string]interface{}{
					"namespace":     "samples-namespace",
					"taskQueueName": "sample-task-queue",
				},
			},
			filters: map[Filter]interface{}{
				Namespace:     "samples-namespace",
				TaskQueueName: "sample-task-queue",
			},
			matched: true,
		},
		{
			v: &constrainedValue{
				Constraints: map[string]interface{}{
					"namespace":         "samples-namespace",
					"some-other-filter": "sample-task-queue",
				},
			},
			filters: map[Filter]interface{}{
				Namespace:     "samples-namespace",
				TaskQueueName: "sample-task-queue",
			},
			matched: false,
		},
		{
			v: &constrainedValue{
				Constraints: map[string]interface{}{
					"namespace": "samples-namespace",
				},
			},
			filters: map[Filter]interface{}{
				TaskQueueName: "sample-task-queue",
			},
			matched: false,
		},
	}

	for _, tc := range testCases {
		matched := match(tc.v, tc.filters)
		s.Equal(tc.matched, matched)
	}
}

type MockFileInfo struct {
	FileName     string
	IsDirectory  bool
	ModTimeValue time.Time
}

func (mfi MockFileInfo) Name() string       { return mfi.FileName }
func (mfi MockFileInfo) Size() int64        { return int64(8) }
func (mfi MockFileInfo) Mode() os.FileMode  { return os.ModePerm }
func (mfi MockFileInfo) ModTime() time.Time { return mfi.ModTimeValue }
func (mfi MockFileInfo) IsDir() bool        { return mfi.IsDirectory }
func (mfi MockFileInfo) Sys() interface{}   { return nil }

func (s *fileBasedClientSuite) TestUpdate_ChangedValue() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	doneCh := make(chan interface{})
	reader := NewMockfileReader(ctrl)
	mockLogger := log.NewMockLogger(ctrl)

	updateInterval := time.Minute * 5
	originFileInfo := &MockFileInfo{ModTimeValue: time.Now()}
	updatedFileInfo := &MockFileInfo{ModTimeValue: originFileInfo.ModTimeValue.Add(updateInterval + time.Second)}

	originFileData := []byte(`
testGetFloat64PropertyKey:
- value: 12
  constraints: {}

testGetIntPropertyKey:
- value: 1000
  constraints: {}

testGetBoolPropertyKey:
- value: false
  constraints: {}
- value: true
  constraints:
    namespace: global-samples-namespace
- value: true
  constraints:
    namespace: samples-namespace
`)
	updatedFileData := []byte(`
testGetFloat64PropertyKey:
- value: 13
  constraints: {}

testGetIntPropertyKey:
- value: 2000
  constraints: {}

testGetBoolPropertyKey:
- value: true
  constraints: {}
- value: false
  constraints:
    namespace: global-samples-namespace
- value: true
  constraints:
    namespace: samples-namespace
`)

	reader.EXPECT().Stat(gomock.Any()).Return(originFileInfo, nil).Times(2)
	reader.EXPECT().ReadFile(gomock.Any()).Return(originFileData, nil)

	mockLogger.EXPECT().Info(gomock.Any()).Times(6)
	client, err := NewFileBasedClientWithReader(reader,
		&FileBasedClientConfig{
			Filepath:     "anyValue",
			PollInterval: updateInterval,
		}, mockLogger, s.doneCh)
	s.NoError(err)

	reader.EXPECT().Stat(gomock.Any()).Return(updatedFileInfo, nil)
	reader.EXPECT().ReadFile(gomock.Any()).Return(updatedFileData, nil)

	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetfloat64propertykey oldValue: { constraints: {} value: 12 } newValue: { constraints: {} value: 13 }", gomock.Any())
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetintpropertykey oldValue: { constraints: {} value: 1000 } newValue: { constraints: {} value: 2000 }", gomock.Any())
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetboolpropertykey oldValue: { constraints: {} value: false } newValue: { constraints: {} value: true }", gomock.Any())
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetboolpropertykey oldValue: { constraints: {{namespace:global-samples-namespace}} value: true } newValue: { constraints: {{namespace:global-samples-namespace}} value: false }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	client.update()
	s.NoError(err)
	close(doneCh)
}

func (s *fileBasedClientSuite) TestUpdate_ChangedMapValue() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	doneCh := make(chan interface{})
	reader := NewMockfileReader(ctrl)
	mockLogger := log.NewMockLogger(ctrl)

	updateInterval := time.Minute * 5
	originFileInfo := &MockFileInfo{ModTimeValue: time.Now()}
	updatedFileInfo := &MockFileInfo{ModTimeValue: originFileInfo.ModTimeValue.Add(updateInterval + time.Second)}

	originFileData := []byte(`
history.defaultActivityRetryPolicy:
- value:
    InitialIntervalInSeconds: 1
    MaximumIntervalCoefficient: 100.0
    BackoffCoefficient: 3.0
    MaximumAttempts: 0
`)
	updatedFileData := []byte(`
history.defaultActivityRetryPolicy:
- value:
    InitialIntervalInSeconds: 3
    MaximumIntervalCoefficient: 100.0
    BackoffCoefficient: 2.0
    MaximumAttempts: 0
`)

	reader.EXPECT().Stat(gomock.Any()).Return(originFileInfo, nil).Times(2)
	reader.EXPECT().ReadFile(gomock.Any()).Return(originFileData, nil)

	mockLogger.EXPECT().Info(gomock.Any()).Times(2)
	client, err := NewFileBasedClientWithReader(reader,
		&FileBasedClientConfig{
			Filepath:     "anyValue",
			PollInterval: updateInterval,
		}, mockLogger, s.doneCh)
	s.NoError(err)

	reader.EXPECT().Stat(gomock.Any()).Return(updatedFileInfo, nil)
	reader.EXPECT().ReadFile(gomock.Any()).Return(updatedFileData, nil)

	mockLogger.EXPECT().Info("dynamic config changed for the key: history.defaultactivityretrypolicy oldValue: { constraints: {} value: map[BackoffCoefficient:3 InitialIntervalInSeconds:1 MaximumAttempts:0 MaximumIntervalCoefficient:100] } newValue: { constraints: {} value: map[BackoffCoefficient:2 InitialIntervalInSeconds:3 MaximumAttempts:0 MaximumIntervalCoefficient:100] }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	client.update()
	s.NoError(err)
	close(doneCh)
}

func (s *fileBasedClientSuite) TestUpdate_NewEntry() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	doneCh := make(chan interface{})
	reader := NewMockfileReader(ctrl)
	mockLogger := log.NewMockLogger(ctrl)

	updateInterval := time.Minute * 5
	originFileInfo := &MockFileInfo{ModTimeValue: time.Now()}
	updatedFileInfo := &MockFileInfo{ModTimeValue: originFileInfo.ModTimeValue.Add(updateInterval + time.Second)}

	originFileData := []byte(`
testGetFloat64PropertyKey:
- value: 12
  constraints: {}
`)
	updatedFileData := []byte(`
testGetFloat64PropertyKey:
- value: 12
  constraints: {}
- value: 22
  constraints:
    namespace: samples-namespace

testGetIntPropertyKey:
- value: 2000
  constraints: {}
`)

	reader.EXPECT().Stat(gomock.Any()).Return(originFileInfo, nil).Times(2)
	reader.EXPECT().ReadFile(gomock.Any()).Return(originFileData, nil)

	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetfloat64propertykey oldValue: nil newValue: { constraints: {} value: 12 }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	client, err := NewFileBasedClientWithReader(reader,
		&FileBasedClientConfig{
			Filepath:     "anyValue",
			PollInterval: updateInterval,
		}, mockLogger, s.doneCh)
	s.NoError(err)

	reader.EXPECT().Stat(gomock.Any()).Return(updatedFileInfo, nil)
	reader.EXPECT().ReadFile(gomock.Any()).Return(updatedFileData, nil)

	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetfloat64propertykey oldValue: nil newValue: { constraints: {{namespace:samples-namespace}} value: 22 }", gomock.Any())
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetintpropertykey oldValue: nil newValue: { constraints: {} value: 2000 }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	client.update()
	s.NoError(err)
	close(doneCh)
}

func (s *fileBasedClientSuite) TestUpdate_ChangeOrder_ShouldNotWriteLog() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	doneCh := make(chan interface{})
	reader := NewMockfileReader(ctrl)
	mockLogger := log.NewMockLogger(ctrl)

	updateInterval := time.Minute * 5
	originFileInfo := &MockFileInfo{ModTimeValue: time.Now()}
	updatedFileInfo := &MockFileInfo{ModTimeValue: originFileInfo.ModTimeValue.Add(updateInterval + time.Second)}

	originFileData := []byte(`
testGetFloat64PropertyKey:
- value: 12
  constraints: {}
- value: 22
  constraints:
    namespace: samples-namespace
    testConstraint: testConstraintValue

testGetIntPropertyKey:
- value: 2000
  constraints: {}
`)
	updatedFileData := []byte(`
testGetIntPropertyKey:
- value: 2000
  constraints: {}

testGetFloat64PropertyKey:
- value: 22
  constraints:
    testConstraint: testConstraintValue
    namespace: samples-namespace
- value: 12
  constraints: {}
`)

	reader.EXPECT().Stat(gomock.Any()).Return(originFileInfo, nil).Times(2)
	reader.EXPECT().ReadFile(gomock.Any()).Return(originFileData, nil)

	mockLogger.EXPECT().Info(gomock.Any()).Times(4)
	client, err := NewFileBasedClientWithReader(reader,
		&FileBasedClientConfig{
			Filepath:     "anyValue",
			PollInterval: updateInterval,
		}, mockLogger, s.doneCh)
	s.NoError(err)

	reader.EXPECT().Stat(gomock.Any()).Return(updatedFileInfo, nil)
	reader.EXPECT().ReadFile(gomock.Any()).Return(updatedFileData, nil)

	mockLogger.EXPECT().Info(gomock.Any())
	client.update()
	s.NoError(err)
	close(doneCh)
}
