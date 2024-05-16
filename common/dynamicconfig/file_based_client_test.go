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

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
)

// Note: fileBasedClientSuite also heavily tests Collection, since some tests are easier with data
// provided from a file.
type fileBasedClientSuite struct {
	suite.Suite
	*require.Assertions
	client     Client
	collection *Collection
	doneCh     chan interface{}
}

func TestFileBasedClientSuite(t *testing.T) {
	s := new(fileBasedClientSuite)
	suite.Run(t, s)
}

func (s *fileBasedClientSuite) SetupSuite() {
	var err error
	s.doneCh = make(chan interface{})
	logger := log.NewNoopLogger()
	s.client, err = NewFileBasedClient(&FileBasedClientConfig{
		Filepath:     "config/testConfig.yaml",
		PollInterval: time.Second * 5,
	}, logger, s.doneCh)
	s.collection = NewCollection(s.client, logger)
	s.Require().NoError(err)
}

func (s *fileBasedClientSuite) TearDownSuite() {
	close(s.doneCh)
}

func (s *fileBasedClientSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *fileBasedClientSuite) TestGetValue() {
	cvs := s.client.GetValue(testGetBoolPropertyKey)
	s.Equal(3, len(cvs))
	s.ElementsMatch([]ConstrainedValue{
		{Constraints: Constraints{}, Value: false},
		{Constraints: Constraints{Namespace: "global-samples-namespace"}, Value: true},
		{Constraints: Constraints{Namespace: "samples-namespace"}, Value: true},
	}, cvs)
}

func (s *fileBasedClientSuite) TestGetValue_NonExistKey() {
	cvs := s.client.GetValue(unknownKey)
	s.Nil(cvs)

	defaultValue := true
	v := NewGlobalBoolSetting(unknownKey, defaultValue, "").Get(s.collection)()
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetValue_CaseInsensitie() {
	cvs := s.client.GetValue(testCaseInsensitivePropertyKey)
	s.Equal(1, len(cvs))

	v := NewGlobalBoolSetting(testCaseInsensitivePropertyKey, false, "").Get(s.collection)()
	s.Equal(true, v)
}

func (s *fileBasedClientSuite) TestGetIntValue() {
	v := NewGlobalIntSetting(testGetIntPropertyKey, 1, "").Get(s.collection)()
	s.Equal(1000, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterNotMatch() {
	v := NewNamespaceIntSetting(testGetIntPropertyKey, 500, "").Get(s.collection)("samples-namespace")
	s.Equal(1000, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_WrongType() {
	defaultValue := 2000
	v := NewNamespaceIntSetting(testGetIntPropertyKey, defaultValue, "").Get(s.collection)("global-samples-namespace")
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByWorkflowTaskQueueInfo() {
	expectedValue := 1001
	v := NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		"global-samples-namespace", "test-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByNoTaskTypeQueueInfo() {
	expectedValue := 1003
	v := NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		// this is contrived, but simulates something that doesn't match workflow or activity
		"global-samples-namespace", "test-tq", enumspb.TaskQueueType(3),
	)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByActivityTaskQueueInfo() {
	expectedValue := 1002
	v := NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		"global-samples-namespace", "test-tq", enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByTaskQueueNameOnly() {
	expectedValue := 1005
	v := NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		"some-other-namespace", "other-test-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterByTQ_NamespaceOnly() {
	expectedValue := 1004
	v := NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		"another-namespace", "test-tq", 0)
	s.Equal(expectedValue, v)
	expectedValue = 1005
	v = NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		"another-namespace", "other-test-tq", 0)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterByTQ_MatchFallback() {
	// should return 1001 as the most specific match
	v1 := NewTaskQueueIntSetting(testGetIntPropertyKey, 1001, "").Get(s.collection)(
		"global-samples-namespace", "test-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	v2 := NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		"global-samples-namespace", "test-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(v1, v2)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterByDestination() {
	dc := NewDestinationIntSetting(testGetIntPropertyFilteredByDestinationKey, 5, "").Get(s.collection)
	s.Equal(10, dc("foo", "bar"))
	s.Equal(20, dc("test-namespace", "test-destination-1"))
	s.Equal(30, dc("test-namespace", "random-destination"))
	s.Equal(40, dc("random-namespace", "test-destination-1"))
	s.Equal(50, dc("test-namespace", "test-destination-2"))
}

func (s *fileBasedClientSuite) TestGetFloatValue() {
	v := NewGlobalFloatSetting(testGetFloat64PropertyKey, 1, "").Get(s.collection)()
	s.Equal(12.0, v)
}

func (s *fileBasedClientSuite) TestGetFloatValue_WrongType() {
	defaultValue := 1.0
	v := NewNamespaceFloatSetting(testGetFloat64PropertyKey, defaultValue, "").Get(s.collection)("samples-namespace")
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetBoolValue() {
	v := NewGlobalBoolSetting(testGetBoolPropertyKey, true, "").Get(s.collection)()
	s.Equal(false, v)
}

func (s *fileBasedClientSuite) TestGetStringValue() {
	v := NewNamespaceStringSetting(testGetStringPropertyKey, "defaultString", "").Get(s.collection)("random-namespace")
	s.Equal("constrained-string", v)
}

func (s *fileBasedClientSuite) TestGetMapValue() {
	var defaultVal map[string]interface{}
	v := NewGlobalMapSetting(testGetMapPropertyKey, defaultVal, "").Get(s.collection)()
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

func (s *fileBasedClientSuite) TestGetTypedValue() {
	type myStruct struct {
		Number int
		Days   time.Duration
		Inner  struct {
			Key1 float64
			Key2 bool
		}
		UnsetInFile string
	}
	v := NewGlobalTypedSetting(testGetTypedPropertyKey, myStruct{UnsetInFile: "unset"}, "").Get(s.collection)()
	expectedVal := myStruct{
		Number: 23,
		Days:   6 * 24 * time.Hour,
		Inner: struct {
			Key1 float64
			Key2 bool
		}{
			Key1: 12345.0,
			Key2: true,
		},
		UnsetInFile: "unset", // note this value comes from the default
	}
	s.Equal(expectedVal, v)
}

func (s *fileBasedClientSuite) TestGetMapValue_WrongType() {
	var defaultVal map[string]interface{}
	v := NewNamespaceMapSetting(testGetMapPropertyKey, defaultVal, "").Get(s.collection)("random-namespace")
	s.Equal(defaultVal, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue() {
	v := NewGlobalDurationSetting(testGetDurationPropertyKey, time.Second, "").Get(s.collection)()
	s.Equal(time.Minute, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_DefaultSeconds() {
	v := NewNamespaceDurationSetting(testGetDurationPropertyKey, time.Second, "").Get(s.collection)("samples-namespace")
	s.Equal(2*time.Second, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_NotStringRepresentation() {
	v := NewNamespaceDurationSetting(testGetDurationPropertyKey, time.Second, "").Get(s.collection)("broken-namespace")
	s.Equal(time.Second, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_ParseFailed() {
	v := NewTaskQueueDurationSetting(testGetDurationPropertyKey, time.Second, "").Get(s.collection)(
		"samples-namespace", "longIdleTimeTaskqueue", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(time.Second, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_FilteredByTaskTypeQueue() {
	expectedValue := time.Second * 10
	v := NewTaskTypeDurationSetting(testGetDurationPropertyFilteredByTaskTypeKey, 0, "").Get(s.collection)(
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
	)
	s.Equal(expectedValue, v)
	v = NewTaskTypeDurationSetting(testGetDurationPropertyFilteredByTaskTypeKey, 0, "").Get(s.collection)(
		enumsspb.TASK_TYPE_REPLICATION_HISTORY,
	)
	s.Equal(expectedValue, v)
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
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetboolpropertykey oldValue: { constraints: {{Namespace:global-samples-namespace}} value: true } newValue: { constraints: {{Namespace:global-samples-namespace}} value: false }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	s.NoError(client.update())
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
	s.NoError(client.update())
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

	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetfloat64propertykey oldValue: nil newValue: { constraints: {{Namespace:samples-namespace}} value: 22 }", gomock.Any())
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetintpropertykey oldValue: nil newValue: { constraints: {} value: 2000 }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	s.NoError(client.update())
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

	mockLogger.EXPECT().Info(gomock.Any()).Times(1)
	s.NoError(client.update())
	s.NoError(err)
	close(doneCh)
}
