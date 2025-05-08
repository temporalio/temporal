package dynamicconfig_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/retrypolicy"
	"go.uber.org/mock/gomock"
)

// Note: fileBasedClientSuite also heavily tests Collection, since some tests are easier with data
// provided from a file.
type fileBasedClientSuite struct {
	suite.Suite
	*require.Assertions
	client     dynamicconfig.Client
	collection *dynamicconfig.Collection
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
	s.client, err = dynamicconfig.NewFileBasedClient(&dynamicconfig.FileBasedClientConfig{
		Filepath:     "config/testConfig.yaml",
		PollInterval: time.Second * 5,
	}, logger, s.doneCh)
	s.Require().NoError(err)
	s.collection = dynamicconfig.NewCollection(s.client, logger)
	s.collection.Start()
}

func (s *fileBasedClientSuite) TearDownSuite() {
	s.collection.Stop()
	close(s.doneCh)
}

func (s *fileBasedClientSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	dynamicconfig.ResetRegistryForTest()
}

func (s *fileBasedClientSuite) TestGetValue() {
	cvs := s.client.GetValue(testGetBoolPropertyKey)
	s.Equal(3, len(cvs))
	s.ElementsMatch([]dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{}, Value: false},
		{Constraints: dynamicconfig.Constraints{Namespace: "global-samples-namespace"}, Value: true},
		{Constraints: dynamicconfig.Constraints{Namespace: "samples-namespace"}, Value: true},
	}, cvs)
}

func (s *fileBasedClientSuite) TestGetValue_NonExistKey() {
	cvs := s.client.GetValue(unknownKey)
	s.Nil(cvs)

	defaultValue := true
	v := dynamicconfig.NewGlobalBoolSetting(unknownKey, defaultValue, "").Get(s.collection)()
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetValue_CaseInsensitie() {
	cvs := s.client.GetValue(testCaseInsensitivePropertyKey)
	s.Equal(1, len(cvs))

	v := dynamicconfig.NewGlobalBoolSetting(testCaseInsensitivePropertyKey, false, "").Get(s.collection)()
	s.Equal(true, v)
}

func (s *fileBasedClientSuite) TestGetIntValue() {
	v := dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 1, "").Get(s.collection)()
	s.Equal(1000, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterNotMatch() {
	v := dynamicconfig.NewNamespaceIntSetting(testGetIntPropertyKey, 500, "").Get(s.collection)("samples-namespace")
	s.Equal(1000, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_WrongType() {
	defaultValue := 2000
	v := dynamicconfig.NewNamespaceIntSetting(testGetIntPropertyKey, defaultValue, "").Get(s.collection)("global-samples-namespace")
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByWorkflowTaskQueueInfo() {
	expectedValue := 1001
	v := dynamicconfig.NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		"global-samples-namespace", "test-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByNoTaskTypeQueueInfo() {
	expectedValue := 1003
	v := dynamicconfig.NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		// this is contrived, but simulates something that doesn't match workflow or activity
		"global-samples-namespace", "test-tq", enumspb.TaskQueueType(3),
	)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByActivityTaskQueueInfo() {
	expectedValue := 1002
	v := dynamicconfig.NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		"global-samples-namespace", "test-tq", enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByTaskQueueNameOnly() {
	expectedValue := 1005
	v := dynamicconfig.NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "").Get(s.collection)(
		"some-other-namespace", "other-test-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterByTQ_NamespaceOnly() {
	expectedValue := 1004
	setting := dynamicconfig.NewTaskQueueIntSetting(testGetIntPropertyKey, 0, "")
	v := setting.Get(s.collection)("another-namespace", "test-tq", 0)
	s.Equal(expectedValue, v)
	expectedValue = 1005
	v = setting.Get(s.collection)("another-namespace", "other-test-tq", 0)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterByTQ_MatchFallback() {
	// should return 1001 as the most specific match
	setting := dynamicconfig.NewTaskQueueIntSetting(testGetIntPropertyKey, 1001, "")
	v1 := setting.Get(s.collection)(
		"global-samples-namespace", "test-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	v2 := setting.WithDefault(0).Get(s.collection)(
		"global-samples-namespace", "test-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(v1, v2)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilterByDestination() {
	dc := dynamicconfig.NewDestinationIntSetting(testGetIntPropertyFilteredByDestinationKey, 5, "").Get(s.collection)
	s.Equal(10, dc("foo", "bar"))
	s.Equal(20, dc("test-namespace", "test-destination-1"))
	s.Equal(30, dc("test-namespace", "random-destination"))
	s.Equal(40, dc("random-namespace", "test-destination-1"))
	s.Equal(50, dc("test-namespace", "test-destination-2"))
}

func (s *fileBasedClientSuite) TestGetFloatValue() {
	v := dynamicconfig.NewGlobalFloatSetting(testGetFloat64PropertyKey, 1, "").Get(s.collection)()
	s.Equal(12.0, v)
}

func (s *fileBasedClientSuite) TestGetFloatValue_WrongType() {
	defaultValue := 1.0
	v := dynamicconfig.NewNamespaceFloatSetting(testGetFloat64PropertyKey, defaultValue, "").Get(s.collection)("samples-namespace")
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetBoolValue() {
	v := dynamicconfig.NewGlobalBoolSetting(testGetBoolPropertyKey, true, "").Get(s.collection)()
	s.Equal(false, v)
}

func (s *fileBasedClientSuite) TestGetStringValue() {
	v := dynamicconfig.NewNamespaceStringSetting(testGetStringPropertyKey, "defaultString", "").Get(s.collection)("random-namespace")
	s.Equal("constrained-string", v)
}

func (s *fileBasedClientSuite) TestGetMapValue() {
	var defaultVal map[string]interface{}
	v := dynamicconfig.NewGlobalMapSetting(testGetMapPropertyKey, defaultVal, "").Get(s.collection)()
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
	v := dynamicconfig.NewGlobalTypedSetting(testGetTypedPropertyKey, myStruct{UnsetInFile: "unset"}, "").Get(s.collection)()
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
	v := dynamicconfig.NewNamespaceMapSetting(testGetMapPropertyKey, defaultVal, "").Get(s.collection)("random-namespace")
	s.Equal(defaultVal, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue() {
	v := dynamicconfig.NewGlobalDurationSetting(testGetDurationPropertyKey, time.Second, "").Get(s.collection)()
	s.Equal(time.Minute, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_DefaultSeconds() {
	v := dynamicconfig.NewNamespaceDurationSetting(testGetDurationPropertyKey, time.Second, "").Get(s.collection)("samples-namespace")
	s.Equal(2*time.Second, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_NotStringRepresentation() {
	v := dynamicconfig.NewNamespaceDurationSetting(testGetDurationPropertyKey, time.Second, "").Get(s.collection)("broken-namespace")
	s.Equal(time.Second, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_ParseFailed() {
	v := dynamicconfig.NewTaskQueueDurationSetting(testGetDurationPropertyKey, time.Second, "").Get(s.collection)(
		"samples-namespace", "longIdleTimeTaskqueue", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(time.Second, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_FilteredByTaskTypeQueue() {
	expectedValue := time.Second * 10
	setting := dynamicconfig.NewTaskTypeDurationSetting(testGetDurationPropertyFilteredByTaskTypeKey, 0, "")
	v := setting.Get(s.collection)(enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER)
	s.Equal(expectedValue, v)
	v = setting.Get(s.collection)(enumsspb.TASK_TYPE_REPLICATION_HISTORY)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestValidateConfig_ConfigNotExist() {
	_, err := dynamicconfig.NewFileBasedClient(nil, nil, nil)
	s.Error(err)
}

func (s *fileBasedClientSuite) TestValidateConfig_FileNotExist() {
	_, err := dynamicconfig.NewFileBasedClient(&dynamicconfig.FileBasedClientConfig{
		Filepath:     "file/not/exist.yaml",
		PollInterval: time.Second * 10,
	}, nil, nil)
	s.Error(err)
}

func (s *fileBasedClientSuite) TestValidateConfig_ShortPollInterval() {
	_, err := dynamicconfig.NewFileBasedClient(&dynamicconfig.FileBasedClientConfig{
		Filepath:     "config/testConfig.yaml",
		PollInterval: time.Second,
	}, nil, nil)
	s.Error(err)
}

func (s *fileBasedClientSuite) TestUpdate_ChangedValue() {
	dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 0, "")
	dynamicconfig.NewNamespaceFloatSetting(testGetFloat64PropertyKey, 0, "")
	setting := dynamicconfig.NewNamespaceBoolSetting(testGetBoolPropertyKey, false, "")

	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	doneCh := make(chan interface{})
	reader := dynamicconfig.NewMockFileReader(ctrl)
	mockLogger := log.NewMockLogger(ctrl)

	updateInterval := time.Minute * 5
	originModTime := time.Now()
	updatedModTime := originModTime.Add(updateInterval + time.Second)

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

	reader.EXPECT().GetModTime().Return(originModTime, nil).Times(2)
	reader.EXPECT().ReadFile().Return(originFileData, nil)

	mockLogger.EXPECT().Debug(gomock.Any(), tag.Key(dynamicconfig.DynamicConfigSubscriptionCallback.Key().String()), gomock.Any()).AnyTimes()
	mockLogger.EXPECT().Info(gomock.Any()).Times(6)
	client, err := dynamicconfig.NewFileBasedClientWithReader(reader,
		&dynamicconfig.FileBasedClientConfig{
			Filepath:     "anyValue",
			PollInterval: updateInterval,
		}, mockLogger, doneCh)
	s.NoError(err)

	c := dynamicconfig.NewCollection(client, mockLogger)
	c.Start()
	sub := setting.Subscribe(c)

	val1 := make(chan bool, 1)
	initial1, cancel1 := sub("global-samples-namespace", func(n bool) { val1 <- n })
	s.True(initial1)
	val2 := make(chan bool, 1)
	initial2, cancel2 := sub("other-namespace", func(n bool) { val2 <- n })
	s.False(initial2)

	reader.EXPECT().GetModTime().Return(updatedModTime, nil)
	reader.EXPECT().ReadFile().Return(updatedFileData, nil)

	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetfloat64propertykey oldValue: { constraints: {} value: 12 } newValue: { constraints: {} value: 13 }", gomock.Any())
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetintpropertykey oldValue: { constraints: {} value: 1000 } newValue: { constraints: {} value: 2000 }", gomock.Any())
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetboolpropertykey oldValue: { constraints: {} value: false } newValue: { constraints: {} value: true }", gomock.Any())
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetboolpropertykey oldValue: { constraints: {{Namespace:global-samples-namespace}} value: true } newValue: { constraints: {{Namespace:global-samples-namespace}} value: false }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	s.NoError(client.Update())

	s.False(<-val1)
	s.True(<-val2)
	cancel1()
	cancel2()
	c.Stop()

	close(doneCh)
}

func (s *fileBasedClientSuite) TestUpdate_ChangedTypedValue() {
	dynamicconfig.NewNamespaceTypedSetting("history.fakeRetryPolicy", retrypolicy.DefaultDefaultRetrySettings, "")

	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	doneCh := make(chan interface{})
	reader := dynamicconfig.NewMockFileReader(ctrl)
	mockLogger := log.NewMockLogger(ctrl)

	updateInterval := time.Minute * 5
	originModTime := time.Now()
	updatedModTime := originModTime.Add(updateInterval + time.Second)

	originFileData := []byte(`
history.fakeRetryPolicy:
- value:
    InitialIntervalInSeconds: 1
    MaximumIntervalCoefficient: 100.0
    BackoffCoefficient: 3.0
    MaximumAttempts: 0
`)
	updatedFileData := []byte(`
history.fakeRetryPolicy:
- value:
    InitialIntervalInSeconds: 3
    MaximumIntervalCoefficient: 100.0
    BackoffCoefficient: 2.0
    MaximumAttempts: 0
`)

	reader.EXPECT().GetModTime().Return(originModTime, nil).Times(2)
	reader.EXPECT().ReadFile().Return(originFileData, nil)

	mockLogger.EXPECT().Info(gomock.Any()).Times(2)
	client, err := dynamicconfig.NewFileBasedClientWithReader(reader,
		&dynamicconfig.FileBasedClientConfig{
			Filepath:     "anyValue",
			PollInterval: updateInterval,
		}, mockLogger, s.doneCh)
	s.NoError(err)

	reader.EXPECT().GetModTime().Return(updatedModTime, nil)
	reader.EXPECT().ReadFile().Return(updatedFileData, nil)

	mockLogger.EXPECT().Info("dynamic config changed for the key: history.fakeretrypolicy oldValue: { constraints: {} value: map[BackoffCoefficient:3 InitialIntervalInSeconds:1 MaximumAttempts:0 MaximumIntervalCoefficient:100] } newValue: { constraints: {} value: map[BackoffCoefficient:2 InitialIntervalInSeconds:3 MaximumAttempts:0 MaximumIntervalCoefficient:100] }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	s.NoError(client.Update())
	s.NoError(err)
	close(doneCh)
}

func (s *fileBasedClientSuite) TestUpdate_NewEntry() {
	dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 0, "")
	dynamicconfig.NewNamespaceFloatSetting(testGetFloat64PropertyKey, 0, "")

	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	doneCh := make(chan interface{})
	reader := dynamicconfig.NewMockFileReader(ctrl)
	mockLogger := log.NewMockLogger(ctrl)

	updateInterval := time.Minute * 5
	originModTime := time.Now()
	updatedModTime := originModTime.Add(updateInterval + time.Second)

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

	reader.EXPECT().GetModTime().Return(originModTime, nil).Times(2)
	reader.EXPECT().ReadFile().Return(originFileData, nil)

	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetfloat64propertykey oldValue: nil newValue: { constraints: {} value: 12 }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	client, err := dynamicconfig.NewFileBasedClientWithReader(reader,
		&dynamicconfig.FileBasedClientConfig{
			Filepath:     "anyValue",
			PollInterval: updateInterval,
		}, mockLogger, s.doneCh)
	s.NoError(err)

	reader.EXPECT().GetModTime().Return(updatedModTime, nil)
	reader.EXPECT().ReadFile().Return(updatedFileData, nil)

	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetfloat64propertykey oldValue: nil newValue: { constraints: {{Namespace:samples-namespace}} value: 22 }", gomock.Any())
	mockLogger.EXPECT().Info("dynamic config changed for the key: testgetintpropertykey oldValue: nil newValue: { constraints: {} value: 2000 }", gomock.Any())
	mockLogger.EXPECT().Info(gomock.Any())
	s.NoError(client.Update())
	s.NoError(err)
	close(doneCh)
}

func (s *fileBasedClientSuite) TestUpdate_ChangeOrder_ShouldNotWriteLog() {
	dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 0, "")
	dynamicconfig.NewNamespaceFloatSetting(testGetFloat64PropertyKey, 0, "")

	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	doneCh := make(chan interface{})
	reader := dynamicconfig.NewMockFileReader(ctrl)
	mockLogger := log.NewMockLogger(ctrl)

	updateInterval := time.Minute * 5
	originModTime := time.Now()
	updatedModTime := originModTime.Add(updateInterval + time.Second)

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

	reader.EXPECT().GetModTime().Return(originModTime, nil).Times(2)
	reader.EXPECT().ReadFile().Return(originFileData, nil)

	mockLogger.EXPECT().Info(gomock.Any()).Times(4)
	client, err := dynamicconfig.NewFileBasedClientWithReader(reader,
		&dynamicconfig.FileBasedClientConfig{
			Filepath:     "anyValue",
			PollInterval: updateInterval,
		}, mockLogger, s.doneCh)
	s.NoError(err)

	reader.EXPECT().GetModTime().Return(updatedModTime, nil)
	reader.EXPECT().ReadFile().Return(updatedFileData, nil)

	mockLogger.EXPECT().Info(gomock.Any()).Times(1)
	s.NoError(client.Update())
	s.NoError(err)
	close(doneCh)
}

func (s *fileBasedClientSuite) TestWarnUnregisteredKey() {
	dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 0, "")

	lr := dynamicconfig.ValidateFile([]byte(`
testGetIntPropertyKey:
- value: 2000
testGetFloat64PropertyKey:
- value: 22.222
`))
	s.Empty(lr.Errors)
	s.Equal(1, len(lr.Warnings))
	s.ErrorContains(lr.Warnings[0], `unregistered key "testGetFloat64PropertyKey"`)
}

func (s *fileBasedClientSuite) TestWarnValidationInt() {
	dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 0, "")

	lr := dynamicconfig.ValidateFile([]byte(`
testGetIntPropertyKey:
- value: not a number
`))
	s.Empty(lr.Errors)
	s.Equal(1, len(lr.Warnings))
	s.ErrorContains(lr.Warnings[0], `validation failed: key "testGetIntPropertyKey" value not a number: value type is not int`)
}

func (s *fileBasedClientSuite) TestWarnConstraint() {
	dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 0, "")

	lr := dynamicconfig.ValidateFile([]byte(`
testGetIntPropertyKey:
- value: 5005
  constraints:
    namespace: samples-namespace
`))
	s.Empty(lr.Errors)
	s.Equal(1, len(lr.Warnings))
	s.ErrorContains(lr.Warnings[0], `constraint "namespace" isn't valid for dynamic config key "testGetIntPropertyKey"`)
}

func (s *fileBasedClientSuite) TestWarnMultiple() {
	dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 0, "")

	lr := dynamicconfig.ValidateFile([]byte(`
unknownKey:
- value: "5d"
testGetIntPropertyKey:
- value: not a number
  constraints:
    namespace: samples-namespace
`))
	s.Empty(lr.Errors)
	s.Equal(3, len(lr.Warnings))
}

func (s *fileBasedClientSuite) TestErrorYamlDecode() {
	lr := dynamicconfig.ValidateFile([]byte(`}}}}}}}}}`))
	s.Equal(1, len(lr.Errors))
	s.ErrorContains(lr.Errors[0], "decode error")
}

func (s *fileBasedClientSuite) TestErrorBadConstraint() {
	dynamicconfig.NewNamespaceBoolSetting(testGetBoolPropertyKey, true, "")

	lr := dynamicconfig.ValidateFile([]byte(`
testGetBoolPropertyKey:
- value: false
  constraints:
    namespace: 35
`))
	s.Equal(1, len(lr.Errors))
	s.ErrorContains(lr.Errors[0], "namespace constraint must be string")
}

func (s *fileBasedClientSuite) TestErrorBadConstraints() {
	dynamicconfig.NewTaskQueueBoolSetting(testGetBoolPropertyKey, true, "")

	lr := dynamicconfig.ValidateFile([]byte(`
testGetBoolPropertyKey:
- value: false
  constraints:
    taskQueueName: 22
    taskType: invalid
- value: true
  constraints:
    color: blue
`))
	found := 0
	for _, err := range lr.Errors {
		e := err.Error()
		switch {
		case strings.Contains(e, "taskQueueName constraint must be string"):
			found++
		case strings.Contains(e, "invalid is not a valid TaskQueueType"):
			found++
		case strings.Contains(e, `unknown constraint type "color"`):
			found++
		}
	}
	s.Equal(3, found)
}
