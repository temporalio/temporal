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

package history

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	commandAttrValidatorSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockNamespaceCache *cache.MockNamespaceCache

		validator *commandAttrValidator

		testNamespaceID       string
		testTargetNamespaceID string
	}
)

func TestCommandAttrValidatorSuite(t *testing.T) {
	s := new(commandAttrValidatorSuite)
	suite.Run(t, s)
}

func (s *commandAttrValidatorSuite) SetupSuite() {
	s.testNamespaceID = "test namespace ID"
	s.testTargetNamespaceID = "test target namespace ID"
}

func (s *commandAttrValidatorSuite) TearDownSuite() {
}

func (s *commandAttrValidatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockNamespaceCache = cache.NewMockNamespaceCache(s.controller)
	config := &Config{
		MaxIDLengthLimit:                  dynamicconfig.GetIntPropertyFn(1000),
		ValidSearchAttributes:             dynamicconfig.GetMapPropertyFn(definition.GetDefaultIndexedKeys()),
		SearchAttributesNumberOfKeysLimit: dynamicconfig.GetIntPropertyFilteredByNamespace(100),
		SearchAttributesSizeOfValueLimit:  dynamicconfig.GetIntPropertyFilteredByNamespace(2 * 1024),
		SearchAttributesTotalSizeLimit:    dynamicconfig.GetIntPropertyFilteredByNamespace(40 * 1024),
		DefaultActivityRetryPolicy:        dynamicconfig.GetMapPropertyFn(getDefaultActivityRetryPolicyConfigOptions()),
	}
	s.validator = newCommandAttrValidator(
		s.mockNamespaceCache,
		config,
		log.NewNoop(),
	)
}

func (s *commandAttrValidatorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *commandAttrValidatorSuite) TestValidateSignalExternalWorkflowExecutionAttributes() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).AnyTimes()

	var attributes *commandpb.SignalExternalWorkflowExecutionCommandAttributes

	err := s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "SignalExternalWorkflowExecutionCommandAttributes is not set on command.")

	attributes = &commandpb.SignalExternalWorkflowExecutionCommandAttributes{}
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "Execution is nil on command.")

	attributes.Execution = &commonpb.WorkflowExecution{}
	attributes.Execution.WorkflowId = "workflow-id"
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "SignalName is not set on command.")

	attributes.Execution.RunId = "run-id"
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "Invalid RunId set on command.")
	attributes.Execution.RunId = testRunID

	attributes.SignalName = "my signal name"
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.NoError(err)

	attributes.Input = payloads.EncodeString("test input")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.NoError(err)
}

func (s *commandAttrValidatorSuite) TestValidateUpsertWorkflowSearchAttributes() {
	namespace := "testNamespace"
	var attributes *commandpb.UpsertWorkflowSearchAttributesCommandAttributes

	err := s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.EqualError(err, "UpsertWorkflowSearchAttributesCommandAttributes is not set on command.")

	attributes = &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{}
	err = s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.EqualError(err, "SearchAttributes is not set on command.")

	attributes.SearchAttributes = &commonpb.SearchAttributes{}
	err = s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.EqualError(err, "IndexedFields is empty on command.")

	attributes.SearchAttributes.IndexedFields = map[string]*commonpb.Payload{"CustomKeywordField": payload.EncodeString("bytes")}
	err = s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToLocal() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToEffectiveLocal_SameCluster() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToEffectiveLocal_DiffCluster() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []string{cluster.TestAlternativeClusterName},
		},
		1234,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToGlobal() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToLocal_SameCluster() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToLocal_DiffCluster() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []string{cluster.TestAlternativeClusterName},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToEffectiveLocal_SameCluster() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		5678,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToEffectiveLocal_DiffCluster() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []string{cluster.TestAlternativeClusterName},
		},
		5678,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToGlobal() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
			},
		},
		5678,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToLocal() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToEffectiveLocal() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		5678,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
			},
		},
		1234,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToGlobal_DiffNamespace() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestAlternativeClusterName,
				cluster.TestCurrentClusterName,
			},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToGlobal_SameNamespace() {
	targetNamespaceID := s.testNamespaceID

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, targetNamespaceID)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateTaskQueueName() {
	newTaskQueue := func(name string) *taskqueuepb.TaskQueue {
		return &taskqueuepb.TaskQueue{
			Name: name,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
	}

	testCases := []struct {
		defaultVal  string
		input       *taskqueuepb.TaskQueue
		output      *taskqueuepb.TaskQueue
		isOutputErr bool
	}{
		{"tq-1", nil, newTaskQueue("tq-1"), false},
		{"", newTaskQueue("tq-1"), newTaskQueue("tq-1"), false},
		{"tq-1", newTaskQueue("tq-1"), newTaskQueue("tq-1"), false},
		{"", newTaskQueue("/tl-1"), newTaskQueue("/tl-1"), false},
		{"", newTaskQueue("/__temporal_sys"), newTaskQueue("/__temporal_sys"), false},
		{"", nil, newTaskQueue(""), true},
		{"", newTaskQueue(""), newTaskQueue(""), true},
		{"", newTaskQueue(reservedTaskQueuePrefix), newTaskQueue(reservedTaskQueuePrefix), true},
		{"tq-1", newTaskQueue(reservedTaskQueuePrefix), newTaskQueue(reservedTaskQueuePrefix), true},
		{"", newTaskQueue(reservedTaskQueuePrefix + "tq-1"), newTaskQueue(reservedTaskQueuePrefix + "tq-1"), true},
		{"tq-1", newTaskQueue(reservedTaskQueuePrefix + "tq-1"), newTaskQueue(reservedTaskQueuePrefix + "tq-1"), true},
	}

	for _, tc := range testCases {
		key := tc.defaultVal + "#"
		if tc.input != nil {
			key += tc.input.GetName()
		} else {
			key += "nil"
		}
		s.Run(key, func() {
			output, err := s.validator.validateTaskQueue(tc.input, tc.defaultVal)
			if tc.isOutputErr {
				s.Error(err)
			} else {
				s.NoError(err)
			}
			s.EqualValues(tc.output, output)
		})
	}
}

func (s *commandAttrValidatorSuite) TestValidateActivityRetryPolicy() {
	testCases := []struct {
		name  string
		input *commonpb.RetryPolicy
		want  *commonpb.RetryPolicy
	}{
		{
			name:  "override non-set policy",
			input: nil,
			want: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(1 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    timestamp.DurationPtr(100 * time.Second),
				MaximumAttempts:    0,
			},
		},
		{
			name: "do not override fully set policy",
			input: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(5 * time.Second),
				BackoffCoefficient: 10,
				MaximumInterval:    timestamp.DurationPtr(20 * time.Second),
				MaximumAttempts:    8,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(5 * time.Second),
				BackoffCoefficient: 10,
				MaximumInterval:    timestamp.DurationPtr(20 * time.Second),
				MaximumAttempts:    8,
			},
		},
		{
			name: "partial override of fields",
			input: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(0 * time.Second),
				BackoffCoefficient: 1.2,
				MaximumInterval:    timestamp.DurationPtr(0 * time.Second),
				MaximumAttempts:    7,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(1 * time.Second),
				BackoffCoefficient: 1.2,
				MaximumInterval:    timestamp.DurationPtr(100 * time.Second),
				MaximumAttempts:    7,
			},
		},
		{
			name: "set expected max interval if only init interval set",
			input: &commonpb.RetryPolicy{
				InitialInterval: timestamp.DurationPtr(3 * time.Second),
				MaximumInterval: timestamp.DurationPtr(0 * time.Second),
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(3 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    timestamp.DurationPtr(300 * time.Second),
				MaximumAttempts:    0,
			},
		},
		{
			name: "override all defaults",
			input: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(0 * time.Second),
				BackoffCoefficient: 0,
				MaximumInterval:    timestamp.DurationPtr(0 * time.Second),
				MaximumAttempts:    0,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    timestamp.DurationPtr(1 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    timestamp.DurationPtr(100 * time.Second),
				MaximumAttempts:    0,
			},
		},
	}

	for _, tt := range testCases {
		s.Run(tt.name, func() {
			attr := &commandpb.ScheduleActivityTaskCommandAttributes{
				RetryPolicy: tt.input,
			}

			err := s.validator.validateActivityRetryPolicy(attr)
			assert.Nil(s.T(), err, "expected no error")
			assert.Equal(s.T(), tt.want, attr.RetryPolicy, "unexpected retry policy")
		})
	}
}
