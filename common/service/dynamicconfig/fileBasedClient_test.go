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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
)

type fileBasedClientSuite struct {
	suite.Suite
	*require.Assertions
	client Client
	doneCh chan struct{}
}

func TestFileBasedClientSuite(t *testing.T) {
	s := new(fileBasedClientSuite)
	suite.Run(t, s)
}

func (s *fileBasedClientSuite) SetupSuite() {
	var err error
	s.doneCh = make(chan struct{})
	s.client, err = NewFileBasedClient(&FileBasedClientConfig{
		Filepath:     "config/testConfig.yaml",
		PollInterval: time.Second * 5,
	}, log.NewNoop(), s.doneCh)
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
	v, err := s.client.GetValue(lastKeyForTest, defaultValue)
	s.Error(err)
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetValueWithFilters() {
	filters := map[Filter]interface{}{
		Namespace: "global-samples-namespace",
	}
	v, err := s.client.GetValueWithFilters(testGetBoolPropertyKey, filters, false)
	s.NoError(err)
	s.Equal(true, v)

	filters = map[Filter]interface{}{
		Namespace: "non-exist-namespace",
	}
	v, err = s.client.GetValueWithFilters(testGetBoolPropertyKey, filters, true)
	s.NoError(err)
	s.Equal(false, v)

	filters = map[Filter]interface{}{
		Namespace:     "samples-namespace",
		TaskQueueName: "non-exist-taskqueue",
	}
	v, err = s.client.GetValueWithFilters(testGetBoolPropertyKey, filters, false)
	s.NoError(err)
	s.Equal(false, v)
}

func (s *fileBasedClientSuite) TestGetValueWithFilters_UnknownFilter() {
	filters := map[Filter]interface{}{
		Namespace:     "global-samples-namespace",
		unknownFilter: "unknown-filter",
	}
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
	filters := map[Filter]interface{}{
		Namespace: "samples-namespace",
	}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, 500)
	s.NoError(err)
	s.Equal(1000, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_WrongType() {
	defaultValue := 2000
	filters := map[Filter]interface{}{
		Namespace: "global-samples-namespace",
	}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, defaultValue)
	s.Error(err)
	s.Equal(defaultValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByWorkflowTaskQueueInfo() {
	expectedValue := 1001
	filters := map[Filter]interface{}{
		Namespace:     "global-samples-namespace",
		TaskQueueName: "test-tq",
		TaskType:      "Workflow",
	}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, 0)
	s.NoError(err)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetIntValue_FilteredByActivityTaskQueueInfo() {
	expectedValue := 1002
	filters := map[Filter]interface{}{
		Namespace:     "global-samples-namespace",
		TaskQueueName: "test-tq",
		TaskType:      "Activity",
	}
	v, err := s.client.GetIntValue(testGetIntPropertyKey, filters, 0)
	s.NoError(err)
	s.Equal(expectedValue, v)
}

func (s *fileBasedClientSuite) TestGetFloatValue() {
	v, err := s.client.GetFloatValue(testGetFloat64PropertyKey, nil, 1)
	s.NoError(err)
	s.Equal(12.0, v)
}

func (s *fileBasedClientSuite) TestGetFloatValue_WrongType() {
	filters := map[Filter]interface{}{
		Namespace: "samples-namespace",
	}
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
	filters := map[Filter]interface{}{
		TaskQueueName: "random taskqueue",
	}
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
	filters := map[Filter]interface{}{
		TaskQueueName: "random taskqueue",
	}
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
	filters := map[Filter]interface{}{
		Namespace: "samples-namespace",
	}
	v, err := s.client.GetDurationValue(testGetDurationPropertyKey, filters, time.Second)
	s.Error(err)
	s.Equal(time.Second, v)
}

func (s *fileBasedClientSuite) TestGetDurationValue_ParseFailed() {
	filters := map[Filter]interface{}{
		Namespace:     "samples-namespace",
		TaskQueueName: "longIdleTimeTaskqueue",
	}
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

func (s *fileBasedClientSuite) TestUpdateConfig() {
	client := s.client.(*fileBasedClient)
	key := ValidSearchAttributes

	// pre-check existing config
	current, err := client.GetMapValue(key, nil, nil)
	s.NoError(err)
	currentNamespaceVal, ok := current["NamespaceId"]
	s.True(ok)
	s.Equal(1, currentNamespaceVal)
	_, ok = current["WorkflowId"]
	s.False(ok)

	// update config
	v := map[string]interface{}{
		"WorkflowId":  1,
		"NamespaceId": 2,
	}
	err = client.UpdateValue(key, v)
	s.NoError(err)

	// verify update result
	current, err = client.GetMapValue(key, nil, nil)
	s.NoError(err)
	currentNamespaceVal, ok = current["NamespaceId"]
	s.True(ok)
	s.Equal(2, currentNamespaceVal)
	currentWorkflowIDVal, ok := current["WorkflowId"]
	s.True(ok)
	s.Equal(1, currentWorkflowIDVal)

	// revert test file back
	v = map[string]interface{}{
		"NamespaceId": 1,
	}
	err = client.UpdateValue(key, v)
	s.NoError(err)
}
