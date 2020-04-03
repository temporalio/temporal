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

package history

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	filterpb "go.temporal.io/temporal-proto/filter"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	querypb "go.temporal.io/temporal-proto/query"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	versionpb "go.temporal.io/temporal-proto/version"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	decisionAttrValidatorSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockNamespaceCache *cache.MockNamespaceCache

		validator *decisionAttrValidator

		testNamespaceID       string
		testTargetNamespaceID string
	}
)

func TestDecisionAttrValidatorSuite(t *testing.T) {
	s := new(decisionAttrValidatorSuite)
	suite.Run(t, s)
}

func (s *decisionAttrValidatorSuite) SetupSuite() {
	s.testNamespaceID = "test namespace ID"
	s.testTargetNamespaceID = "test target namespace ID"
}

func (s *decisionAttrValidatorSuite) TearDownSuite() {
}

func (s *decisionAttrValidatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockNamespaceCache = cache.NewMockNamespaceCache(s.controller)
	config := &Config{
		MaxIDLengthLimit:                  dynamicconfig.GetIntPropertyFn(1000),
		ValidSearchAttributes:             dynamicconfig.GetMapPropertyFn(definition.GetDefaultIndexedKeys()),
		SearchAttributesNumberOfKeysLimit: dynamicconfig.GetIntPropertyFilteredByNamespace(100),
		SearchAttributesSizeOfValueLimit:  dynamicconfig.GetIntPropertyFilteredByNamespace(2 * 1024),
		SearchAttributesTotalSizeLimit:    dynamicconfig.GetIntPropertyFilteredByNamespace(40 * 1024),
	}
	s.validator = newDecisionAttrValidator(
		s.mockNamespaceCache,
		config,
		log.NewNoop(),
	)
}

func (s *decisionAttrValidatorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *decisionAttrValidatorSuite) TestValidateSignalExternalWorkflowExecutionAttributes() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).AnyTimes()

	var attributes *decisionpb.SignalExternalWorkflowExecutionDecisionAttributes

	err := s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "SignalExternalWorkflowExecutionDecisionAttributes is not set on decision.")

	attributes = &decisionpb.SignalExternalWorkflowExecutionDecisionAttributes{}
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "Execution is nil on decision.")

	attributes.Execution = &executionpb.WorkflowExecution{}
	attributes.Execution.WorkflowId = "workflow-id"
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "SignalName is not set on decision.")

	attributes.Execution.RunId = "run-id"
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.EqualError(err, "Invalid RunId set on decision.")
	attributes.Execution.RunId = testRunID

	attributes.SignalName = "my signal name"
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.NoError(err)

	attributes.Input = []byte("test input")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testNamespaceID, s.testTargetNamespaceID, attributes)
	s.NoError(err)
}

func (s *decisionAttrValidatorSuite) TestValidateUpsertWorkflowSearchAttributes() {
	namespace := "testNamespace"
	var attributes *decisionpb.UpsertWorkflowSearchAttributesDecisionAttributes

	err := s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.EqualError(err, "UpsertWorkflowSearchAttributesDecisionAttributes is not set on decision.")

	attributes = &decisionpb.UpsertWorkflowSearchAttributesDecisionAttributes{}
	err = s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.EqualError(err, "SearchAttributes is not set on decision.")

	attributes.SearchAttributes = &commonpb.SearchAttributes{}
	err = s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.EqualError(err, "IndexedFields is empty on decision.")

	attributes.SearchAttributes.IndexedFields = map[string][]byte{"CustomKeywordField": []byte(`bytes`)}
	err = s.validator.validateUpsertWorkflowSearchAttributes(namespace, attributes)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToLocal() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToEffectiveLocal_SameCluster() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToEffectiveLocal_DiffCluster() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestAlternativeClusterName}},
		},
		1234,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_LocalToGlobal() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
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

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToLocal_SameCluster() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToLocal_DiffCluster() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestAlternativeClusterName}},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToEffectiveLocal_SameCluster() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		5678,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToEffectiveLocal_DiffCluster() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestAlternativeClusterName}},
		},
		5678,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_EffectiveLocalToGlobal() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
			},
		},
		5678,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
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

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToLocal() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(namespaceEntry, nil).Times(1)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testTargetNamespaceID).Return(targetNamespaceEntry, nil).Times(1)

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, s.testTargetNamespaceID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToEffectiveLocal() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		5678,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
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

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToGlobal_DiffNamespace() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestAlternativeClusterName},
				{ClusterName: cluster.TestCurrentClusterName},
			},
		},
		1234,
		nil,
	)
	targetNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistence.NamespaceInfo{Name: s.testTargetNamespaceID},
		nil,
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
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

func (s *decisionAttrValidatorSuite) TestValidateCrossNamespaceCall_GlobalToGlobal_SameNamespace() {
	targetNamespaceID := s.testNamespaceID

	err := s.validator.validateCrossNamespaceCall(s.testNamespaceID, targetNamespaceID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateTaskListName() {
	taskList := func(name string) *tasklistpb.TaskList {
		return &tasklistpb.TaskList{Name: name, Kind: tasklistpb.TaskListKindNormal}
	}

	testCases := []struct {
		defaultVal  string
		input       *tasklistpb.TaskList
		output      *tasklistpb.TaskList
		isOutputErr bool
	}{
		{"tl-1", nil, &tasklistpb.TaskList{Name: "tl-1"}, false},
		{"", taskList("tl-1"), taskList("tl-1"), false},
		{"tl-1", taskList("tl-1"), taskList("tl-1"), false},
		{"", taskList("/tl-1"), taskList("/tl-1"), false},
		{"", taskList("/__temporal_sys"), taskList("/__temporal_sys"), false},
		{"", nil, &tasklistpb.TaskList{}, true},
		{"", taskList(""), taskList(""), true},
		{"", taskList(reservedTaskListPrefix), taskList(reservedTaskListPrefix), true},
		{"tl-1", taskList(reservedTaskListPrefix), taskList(reservedTaskListPrefix), true},
		{"", taskList(reservedTaskListPrefix + "tl-1"), taskList(reservedTaskListPrefix + "tl-1"), true},
		{"tl-1", taskList(reservedTaskListPrefix + "tl-1"), taskList(reservedTaskListPrefix + "tl-1"), true},
	}

	for _, tc := range testCases {
		key := tc.defaultVal + "#"
		if tc.input != nil {
			key += tc.input.GetName()
		} else {
			key += "nil"
		}
		s.Run(key, func() {
			output, err := s.validator.validatedTaskList(tc.input, tc.defaultVal)
			if tc.isOutputErr {
				s.Error(err)
			} else {
				s.NoError(err)
			}
			s.EqualValues(tc.output, output)
		})
	}
}
