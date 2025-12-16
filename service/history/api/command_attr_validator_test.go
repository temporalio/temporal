package api

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
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

		validator *CommandAttrValidator

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
		DefaultActivityRetryPolicy:        func(string) retrypolicy.DefaultRetrySettings { return retrypolicy.DefaultDefaultRetrySettings },
		DefaultWorkflowRetryPolicy:        func(string) retrypolicy.DefaultRetrySettings { return retrypolicy.DefaultDefaultRetrySettings },
		EnableCrossNamespaceCommands:      dynamicconfig.GetBoolPropertyFn(true),
		DefaultWorkflowTaskTimeout:        dynamicconfig.GetDurationPropertyFnFilteredByNamespace(primitives.DefaultWorkflowTaskTimeout),
	}
	s.validator = NewCommandAttrValidator(
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

	fc, err := s.validator.ValidateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, attributes)
	s.EqualError(err, "SignalExternalWorkflowExecutionCommandAttributes is not set on SignalExternalWorkflowExecutionCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes = &commandpb.SignalExternalWorkflowExecutionCommandAttributes{}
	fc, err = s.validator.ValidateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, attributes)
	s.EqualError(err, "Execution is not set on SignalExternalWorkflowExecutionCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes.Execution = &commonpb.WorkflowExecution{}
	attributes.Execution.WorkflowId = "workflow-id"
	fc, err = s.validator.ValidateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, attributes)
	s.EqualError(err, "SignalName is not set on SignalExternalWorkflowExecutionCommand. WorkflowId=workflow-id Namespace= RunId=")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes.Execution.RunId = "run-id"
	fc, err = s.validator.ValidateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, attributes)
	s.EqualError(err, "Invalid RunId set on SignalExternalWorkflowExecutionCommand. WorkflowId=workflow-id Namespace= RunId=run-id SignalName=")
	attributes.Execution.RunId = tests.RunID
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes.SignalName = "my signal name"
	fc, err = s.validator.ValidateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, attributes)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)

	attributes.Input = payloads.EncodeString("test input")
	fc, err = s.validator.ValidateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, attributes)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)
}

func (s *commandAttrValidatorSuite) TestValidateUpsertWorkflowSearchAttributes() {
	namespaceName := namespace.Name("tests.Namespace")
	var attributes *commandpb.UpsertWorkflowSearchAttributesCommandAttributes

	fc, err := s.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
	s.EqualError(err, "UpsertWorkflowSearchAttributesCommandAttributes is not set on UpsertWorkflowSearchAttributesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)

	attributes = &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{}
	fc, err = s.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
	s.EqualError(err, "SearchAttributes is not set on UpsertWorkflowSearchAttributesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)

	attributes.SearchAttributes = &commonpb.SearchAttributes{}
	fc, err = s.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
	s.EqualError(err, "IndexedFields is not set on UpsertWorkflowSearchAttributesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)

	saPayload, err := searchattribute.EncodeValue("bytes", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	s.NoError(err)
	attributes.SearchAttributes.IndexedFields = map[string]*commonpb.Payload{
		"Keyword01": saPayload,
	}
	fc, err = s.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)

	// Predefined Worker-Deployment related SA's should be rejected when they are attempted to be upserted
	deploymentRestrictedAttributes := []string{
		sadefs.TemporalWorkerDeploymentVersion,
		sadefs.TemporalWorkerDeployment,
		sadefs.TemporalWorkflowVersioningBehavior,
	}

	for _, attr := range deploymentRestrictedAttributes {
		attributes.SearchAttributes.IndexedFields = map[string]*commonpb.Payload{
			attr: saPayload,
		}
		fc, err = s.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
		s.EqualError(err, fmt.Sprintf("%s attribute can't be set in SearchAttributes", attr))
		s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)
	}
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
		WorkflowTaskTimeout: durationpb.New(maxWorkflowTaskStartToCloseTimeout * 2),
	}

	executionInfo := &persistencespb.WorkflowExecutionInfo{
		WorkflowTypeName:         workflowTypeName,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: durationpb.New(executionTimeout),
	}

	fc, err := s.validator.ValidateContinueAsNewWorkflowExecutionAttributes(
		tests.Namespace,
		attributes,
		executionInfo,
	)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)

	s.Equal(workflowTypeName, attributes.GetWorkflowType().GetName())
	s.Equal(taskQueue, attributes.GetTaskQueue().GetName())
	s.Equal(executionTimeout, attributes.GetWorkflowRunTimeout().AsDuration())
	s.Equal(maxWorkflowTaskStartToCloseTimeout, attributes.GetWorkflowTaskTimeout().AsDuration())

	// Predefined Worker-Deployment related SA's should be rejected when they are attempted to be set during CAN
	saPayload, _ := searchattribute.EncodeValue([]string{"a"}, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	attributes.SearchAttributes = &commonpb.SearchAttributes{}

	deploymentRestrictedAttributes := []string{
		sadefs.TemporalWorkerDeploymentVersion,
		sadefs.TemporalWorkerDeployment,
		sadefs.TemporalWorkflowVersioningBehavior,
	}

	for _, attr := range deploymentRestrictedAttributes {
		attributes.SearchAttributes.IndexedFields = map[string]*commonpb.Payload{
			attr: saPayload,
		}
		fc, err = s.validator.ValidateContinueAsNewWorkflowExecutionAttributes(
			tests.Namespace,
			attributes,
			executionInfo,
		)
		s.EqualError(err, fmt.Sprintf("invalid SearchAttributes on ContinueAsNewWorkflowExecutionCommand: %s attribute "+
			"can't be set in SearchAttributes. WorkflowType=%s TaskQueue=%s",
			attr, workflowTypeName, attributes.TaskQueue))
		s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)
	}
}

func (s *commandAttrValidatorSuite) TestValidateModifyWorkflowProperties() {
	var attributes *commandpb.ModifyWorkflowPropertiesCommandAttributes

	fc, err := s.validator.ValidateModifyWorkflowProperties(attributes)
	s.EqualError(err, "ModifyWorkflowPropertiesCommandAttributes is not set on ModifyWorkflowPropertiesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, fc)

	// test attributes has at least one non-nil attribute
	attributes = &commandpb.ModifyWorkflowPropertiesCommandAttributes{}
	fc, err = s.validator.ValidateModifyWorkflowProperties(attributes)
	s.EqualError(err, "UpsertedMemo is not set on ModifyWorkflowPropertiesCommand.")
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, fc)

	// test UpsertedMemo cannot be an empty map
	attributes = &commandpb.ModifyWorkflowPropertiesCommandAttributes{
		UpsertedMemo: &commonpb.Memo{},
	}
	fc, err = s.validator.ValidateModifyWorkflowProperties(attributes)
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
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

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", s.testTargetNamespaceID, "test-target-workflow-id")
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToGlobal_SameNamespace() {
	targetNamespaceID := s.testNamespaceID

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, "test-workflow-id", targetNamespaceID, "test-target-workflow-id")
	s.Nil(err)
}

func (s *commandAttrValidatorSuite) TestValidateActivityRetryPolicy() {
	testCases := []struct {
		name  string
		input *commonpb.RetryPolicy
		want  *commonpb.RetryPolicy
	}{
		{
			name:  "override non-set policy",
			input: &commonpb.RetryPolicy{},
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

			err := s.validator.validateActivityRetryPolicy(s.testNamespaceID, attr.GetRetryPolicy())
			assert.Nil(s.T(), err, "expected no error")
			assert.Equal(s.T(), tt.want, attr.RetryPolicy, "unexpected retry policy")
		})
	}
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_NoTerminalCommand() {
	err := s.validator.ValidateCommandSequence(nonTerminalCommands)
	s.NoError(err)
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_ValidTerminalCommand() {
	for _, terminalCommand := range terminalCommands {
		err := s.validator.ValidateCommandSequence(append(nonTerminalCommands, terminalCommand))
		s.NoError(err)
	}
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_InvalidTerminalCommand() {
	for _, terminalCommand := range terminalCommands {
		err := s.validator.ValidateCommandSequence(append(
			[]*commandpb.Command{terminalCommand},
			nonTerminalCommands[int(rand.Int31n(int32(len(nonTerminalCommands))))],
		))
		s.Error(err)
		s.IsType(&serviceerror.InvalidArgument{}, err)
	}
}
