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

package respondworkflowtaskcompleted

import (
	"math/rand"
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
	"google.golang.org/protobuf/types/known/durationpb"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

var (
	nonTerminalCommands = []*commandpb.Command{
		{CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK},
		{CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK},
		{CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER},
		{CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER},
		{CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER},
		{CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES},
		{CommandType: enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES},
	}

	terminalCommands = []*commandpb.Command{
		{CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION},
	}
)

type (
	commandAttrValidatorSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockNamespaceCache    *namespace.MockRegistry
		mockVisibilityManager *manager.MockVisibilityManager

		validator *commandAttrValidator

		testNamespaceID       namespace.ID
		testTargetNamespaceID namespace.ID
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
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)

	s.mockVisibilityManager = manager.NewMockVisibilityManager(s.controller)
	s.mockVisibilityManager.EXPECT().GetIndexName().Return("index-name").AnyTimes()
	s.mockVisibilityManager.EXPECT().
		ValidateCustomSearchAttributes(gomock.Any()).
		DoAndReturn(
			func(searchAttributes map[string]any) (map[string]any, error) {
				return searchAttributes, nil
			},
		).
		AnyTimes()

	config := &configs.Config{
		MaxIDLengthLimit:                  dynamicconfig.GetIntPropertyFn(1000),
		SearchAttributesNumberOfKeysLimit: dynamicconfig.GetIntPropertyFnFilteredByNamespace(100),
		SearchAttributesSizeOfValueLimit:  dynamicconfig.GetIntPropertyFnFilteredByNamespace(2 * 1024),
		SearchAttributesTotalSizeLimit:    dynamicconfig.GetIntPropertyFnFilteredByNamespace(40 * 1024),
		DefaultActivityRetryPolicy:        dynamicconfig.GetMapPropertyFnFilteredByNamespace(retrypolicy.GetDefault()),
		DefaultWorkflowRetryPolicy:        dynamicconfig.GetMapPropertyFnFilteredByNamespace(retrypolicy.GetDefault()),
		EnableCrossNamespaceCommands:      dynamicconfig.GetBoolPropertyFn(true),
		DefaultWorkflowTaskTimeout:        dynamicconfig.GetDurationPropertyFnFilteredByNamespace(primitives.DefaultWorkflowTaskTimeout),
	}
	s.validator = newCommandAttrValidator(
		s.mockNamespaceCache,
		config,
		searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			searchattribute.NewTestMapperProvider(nil),
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
			s.mockVisibilityManager,
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		))
}

func (s *commandAttrValidatorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *commandAttrValidatorSuite) TestValidateSignalExternalWorkflowExecutionAttributes() {
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).AnyTimes()

	var attributes *commandpb.SignalExternalWorkflowExecutionCommandAttributes

	fc, err := s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "SignalExternalWorkflowExecutionCommandAttributes is not set on SignalExternalWorkflowExecutionCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes = &commandpb.SignalExternalWorkflowExecutionCommandAttributes{}
	fc, err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "Execution is not set on SignalExternalWorkflowExecutionCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes.Execution = &commonpb.WorkflowExecution{}
	attributes.Execution.WorkflowId = "workflow-id"
	fc, err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "SignalName is not set on SignalExternalWorkflowExecutionCommand. WorkflowId=workflow-id Namespace= RunId=")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes.Execution.RunId = "run-id"
	fc, err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "Invalid RunId set on SignalExternalWorkflowExecutionCommand. WorkflowId=workflow-id Namespace= RunId=run-id SignalName=")
	attributes.Execution.RunId = tests.RunID
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes.SignalName = "my signal name"
	fc, err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)

	attributes.Input = payloads.EncodeString("test input")
	fc, err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)
}

func (s *commandAttrValidatorSuite) TestValidateUpsertWorkflowSearchAttributes() {
	namespace := namespace.Name("tests.Namespace")
	var attributes *commandpb.UpsertWorkflowSearchAttributesCommandAttributes

	fc, err := s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.EqualError(err, "UpsertWorkflowSearchAttributesCommandAttributes is not set on UpsertWorkflowSearchAttributesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)

	attributes = &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{}
	fc, err = s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.EqualError(err, "SearchAttributes is not set on UpsertWorkflowSearchAttributesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)

	attributes.SearchAttributes = &commonpb.SearchAttributes{}
	fc, err = s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.EqualError(err, "IndexedFields is not set on UpsertWorkflowSearchAttributesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)

	saPayload, err := searchattribute.EncodeValue("bytes", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	s.NoError(err)
	attributes.SearchAttributes.IndexedFields = map[string]*commonpb.Payload{
		"CustomKeywordField": saPayload,
	}
	fc, err = s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)
}

func (s *commandAttrValidatorSuite) TestValidateContinueAsNewWorkflowExecutionAttributes() {
	executionTimeout := time.Hour
	workflowTypeName := "workflowType"
	taskQueue := "taskQueue"

	attributes := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
		// workflow type name and task queue name should be retrieved from existing workflow info

		// WorkflowRunTimeout should be shorten to execution timeout
		WorkflowRunTimeout: durationpb.New(executionTimeout * 2),
		// WorkflowTaskTimeout should be shorten to max workflow task timeout
		WorkflowTaskTimeout: durationpb.New(common.MaxWorkflowTaskStartToCloseTimeout * 2),
	}

	executionInfo := &persistencespb.WorkflowExecutionInfo{
		WorkflowTypeName:         workflowTypeName,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: durationpb.New(executionTimeout),
	}

	fc, err := s.validator.validateContinueAsNewWorkflowExecutionAttributes(
		tests.Namespace,
		attributes,
		executionInfo,
	)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)

	s.Equal(workflowTypeName, attributes.GetWorkflowType().GetName())
	s.Equal(taskQueue, attributes.GetTaskQueue().GetName())
	s.Equal(executionTimeout, attributes.GetWorkflowRunTimeout().AsDuration())
	s.Equal(common.MaxWorkflowTaskStartToCloseTimeout, attributes.GetWorkflowTaskTimeout().AsDuration())
}

func (s *commandAttrValidatorSuite) TestValidateModifyWorkflowProperties() {
	namespace := namespace.Name("tests.Namespace")
	var attributes *commandpb.ModifyWorkflowPropertiesCommandAttributes

	fc, err := s.validator.validateModifyWorkflowProperties(namespace, attributes)
	s.EqualError(err, "ModifyWorkflowPropertiesCommandAttributes is not set on ModifyWorkflowPropertiesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, fc)

	// test attributes has at least one non-nil attribute
	attributes = &commandpb.ModifyWorkflowPropertiesCommandAttributes{}
	fc, err = s.validator.validateModifyWorkflowProperties(namespace, attributes)
	s.EqualError(err, "UpsertedMemo is not set on ModifyWorkflowPropertiesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, fc)

	// test UpsertedMemo cannot be an empty map
	attributes = &commandpb.ModifyWorkflowPropertiesCommandAttributes{
		UpsertedMemo: &commonpb.Memo{},
	}
	fc, err = s.validator.validateModifyWorkflowProperties(namespace, attributes)
	s.EqualError(err, "UpsertedMemo.Fields is not set on ModifyWorkflowPropertiesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, fc)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToLocal() {
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToEffectiveLocal_SameCluster() {
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToEffectiveLocal_DiffCluster() {
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []string{cluster.TestAlternativeClusterName},
		},
		1234,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToGlobal() {
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToLocal_SameCluster() {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToLocal_DiffCluster() {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []string{cluster.TestAlternativeClusterName},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToEffectiveLocal_SameCluster() {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		5678,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToEffectiveLocal_DiffCluster() {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []string{cluster.TestAlternativeClusterName},
		},
		5678,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToGlobal() {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
			},
		},
		5678,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToLocal() {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		cluster.TestCurrentClusterName,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToEffectiveLocal() {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		5678,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
			},
		},
		1234,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToGlobal_DiffNamespace() {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestAlternativeClusterName,
				cluster.TestCurrentClusterName,
			},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: s.testTargetNamespaceID.String()},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil)

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
				InitialInterval:    durationpb.New(1 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    durationpb.New(100 * time.Second),
				MaximumAttempts:    0,
			},
		},
		{
			name: "do not override fully set policy",
			input: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(5 * time.Second),
				BackoffCoefficient: 10,
				MaximumInterval:    durationpb.New(20 * time.Second),
				MaximumAttempts:    8,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(5 * time.Second),
				BackoffCoefficient: 10,
				MaximumInterval:    durationpb.New(20 * time.Second),
				MaximumAttempts:    8,
			},
		},
		{
			name: "partial override of fields",
			input: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(0 * time.Second),
				BackoffCoefficient: 1.2,
				MaximumInterval:    durationpb.New(0 * time.Second),
				MaximumAttempts:    7,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				BackoffCoefficient: 1.2,
				MaximumInterval:    durationpb.New(100 * time.Second),
				MaximumAttempts:    7,
			},
		},
		{
			name: "set expected max interval if only init interval set",
			input: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(3 * time.Second),
				MaximumInterval: durationpb.New(0 * time.Second),
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(3 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    durationpb.New(300 * time.Second),
				MaximumAttempts:    0,
			},
		},
		{
			name: "override all defaults",
			input: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(0 * time.Second),
				BackoffCoefficient: 0,
				MaximumInterval:    durationpb.New(0 * time.Second),
				MaximumAttempts:    0,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    durationpb.New(100 * time.Second),
				MaximumAttempts:    0,
			},
		},
	}

	for _, tt := range testCases {
		s.Run(tt.name, func() {
			attr := &commandpb.ScheduleActivityTaskCommandAttributes{
				RetryPolicy: tt.input,
			}

			err := s.validator.validateActivityRetryPolicy(s.testNamespaceID, attr)
			assert.Nil(s.T(), err, "expected no error")
			assert.Equal(s.T(), tt.want, attr.RetryPolicy, "unexpected retry policy")
		})
	}
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_NoTerminalCommand() {
	err := s.validator.validateCommandSequence(nonTerminalCommands)
	s.NoError(err)
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_ValidTerminalCommand() {
	for _, terminalCommand := range terminalCommands {
		err := s.validator.validateCommandSequence(append(nonTerminalCommands, terminalCommand))
		s.NoError(err)
	}
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_InvalidTerminalCommand() {
	for _, terminalCommand := range terminalCommands {
		err := s.validator.validateCommandSequence(append(
			[]*commandpb.Command{terminalCommand},
			nonTerminalCommands[int(rand.Int31n(int32(len(nonTerminalCommands))))],
		))
		s.Error(err)
		s.IsType(&serviceerror.InvalidArgument{}, err)
	}
}

func TestWorkflowSizeChecker_NumChildWorkflows(t *testing.T) {

	for _, c := range []struct {
		Name                      string
		NumPendingChildExecutions int
		NumPendingActivities      int
		NumPendingCancelRequests  int
		NumPendingSignals         int

		PendingChildExecutionsLimit int
		PendingActivitiesLimit      int
		PendingCancelRequestsLimit  int
		PendingSignalsLimit         int

		ExpectedMetric                  string
		ExpectedChildExecutionsErrorMsg string
		ExpectedActivitiesErrorMsg      string
		ExpectedCancelRequestsErrorMsg  string
		ExpectedSignalsErrorMsg         string
	}{
		{
			Name: "No limits and no data",
		},
		{
			Name:                        "Limits but no workflow data",
			PendingChildExecutionsLimit: 1,
			PendingActivitiesLimit:      1,
			PendingCancelRequestsLimit:  1,
			PendingSignalsLimit:         1,
		},
		{
			Name:                        "Limits not exceeded",
			NumPendingChildExecutions:   1,
			NumPendingActivities:        1,
			NumPendingCancelRequests:    1,
			NumPendingSignals:           1,
			PendingChildExecutionsLimit: 2,
			PendingActivitiesLimit:      2,
			PendingCancelRequestsLimit:  2,
			PendingSignalsLimit:         2,
		},
		{
			Name:                        "Pending child executions limit exceeded",
			NumPendingChildExecutions:   1,
			PendingChildExecutionsLimit: 1,
			ExpectedMetric:              "wf_too_many_pending_child_workflows",
			ExpectedChildExecutionsErrorMsg: "the number of pending child workflow executions, 1, has reached the " +
				"per-workflow limit of 1",
		},
		{
			Name:                       "Pending activities limit exceeded",
			NumPendingActivities:       1,
			PendingActivitiesLimit:     1,
			ExpectedMetric:             "wf_too_many_pending_activities",
			ExpectedActivitiesErrorMsg: "the number of pending activities, 1, has reached the per-workflow limit of 1",
		},
		{
			Name:                       "Pending cancel requests limit exceeded",
			NumPendingCancelRequests:   1,
			PendingCancelRequestsLimit: 1,
			ExpectedMetric:             "wf_too_many_pending_cancel_requests",
			ExpectedCancelRequestsErrorMsg: "the number of pending requests to cancel external workflows, 1, has " +
				"reached the per-workflow limit of 1",
		},
		{
			Name:                "Pending signals limit exceeded",
			NumPendingSignals:   1,
			PendingSignalsLimit: 1,
			ExpectedMetric:      "wf_too_many_pending_external_workflow_signals",
			ExpectedSignalsErrorMsg: "the number of pending signals to external workflows, 1, has reached the " +
				"per-workflow limit of 1",
		},
	} {
		t.Run(c.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mutableState := workflow.NewMockMutableState(ctrl)
			logger := log.NewMockLogger(ctrl)
			metricsHandler := metrics.NewMockHandler(ctrl)

			workflowKey := definition.NewWorkflowKey(
				"test-namespace-id",
				"test-workflow-id",
				"test-run-id",
			)
			mutableState.EXPECT().GetWorkflowKey().Return(workflowKey).AnyTimes()

			executionInfos := make(map[int64]*persistencespb.ChildExecutionInfo)
			activityInfos := make(map[int64]*persistencespb.ActivityInfo)
			requestCancelInfos := make(map[int64]*persistencespb.RequestCancelInfo)
			signalInfos := make(map[int64]*persistencespb.SignalInfo)
			for i := 0; i < c.NumPendingChildExecutions; i++ {
				executionInfos[int64(i)] = new(persistencespb.ChildExecutionInfo)
			}
			for i := 0; i < c.NumPendingActivities; i++ {
				activityInfos[int64(i)] = new(persistencespb.ActivityInfo)
			}
			for i := 0; i < c.NumPendingCancelRequests; i++ {
				requestCancelInfos[int64(i)] = new(persistencespb.RequestCancelInfo)
			}
			for i := 0; i < c.NumPendingSignals; i++ {
				signalInfos[int64(i)] = new(persistencespb.SignalInfo)
			}
			mutableState.EXPECT().GetPendingChildExecutionInfos().Return(executionInfos)
			mutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
			mutableState.EXPECT().GetPendingRequestCancelExternalInfos().Return(requestCancelInfos)
			mutableState.EXPECT().GetPendingSignalExternalInfos().Return(signalInfos)

			if len(c.ExpectedMetric) > 0 {
				counterMetric := metrics.NewMockCounterIface(ctrl)
				metricsHandler.EXPECT().Counter(c.ExpectedMetric).Return(counterMetric)
				counterMetric.EXPECT().Record(int64(1))
			}

			for _, msg := range []string{
				c.ExpectedChildExecutionsErrorMsg,
				c.ExpectedActivitiesErrorMsg,
				c.ExpectedCancelRequestsErrorMsg,
				c.ExpectedSignalsErrorMsg,
			} {
				if len(msg) > 0 {
					logger.EXPECT().Error(msg, gomock.Any()).Do(func(msg string, tags ...tag.Tag) {
						var namespaceID, workflowID, runID interface{}
						for _, t := range tags {
							if t.Key() == "wf-namespace-id" {
								namespaceID = t.Value()
							} else if t.Key() == "wf-id" {
								workflowID = t.Value()
							} else if t.Key() == "wf-run-id" {
								runID = t.Value()
							}
						}
						assert.Equal(t, "test-namespace-id", namespaceID)
						assert.Equal(t, "test-workflow-id", workflowID)
						assert.Equal(t, "test-run-id", runID)
					})
				}
			}

			checker := newWorkflowSizeChecker(workflowSizeLimits{
				numPendingChildExecutionsLimit: c.PendingChildExecutionsLimit,
				numPendingActivitiesLimit:      c.PendingActivitiesLimit,
				numPendingCancelsRequestLimit:  c.PendingCancelRequestsLimit,
				numPendingSignalsLimit:         c.PendingSignalsLimit,
			}, mutableState, nil, metricsHandler, logger)

			err := checker.checkIfNumChildWorkflowsExceedsLimit()
			if len(c.ExpectedChildExecutionsErrorMsg) > 0 {
				require.Error(t, err)
				assert.Equal(t, c.ExpectedChildExecutionsErrorMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}

			err = checker.checkIfNumPendingActivitiesExceedsLimit()
			if len(c.ExpectedActivitiesErrorMsg) > 0 {
				require.Error(t, err)
				assert.Equal(t, c.ExpectedActivitiesErrorMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}

			err = checker.checkIfNumPendingCancelRequestsExceedsLimit()
			if len(c.ExpectedCancelRequestsErrorMsg) > 0 {
				require.Error(t, err)
				assert.Equal(t, c.ExpectedCancelRequestsErrorMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}

			err = checker.checkIfNumPendingSignalsExceedsLimit()
			if len(c.ExpectedSignalsErrorMsg) > 0 {
				require.Error(t, err)
				assert.Equal(t, c.ExpectedSignalsErrorMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
