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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
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

		controller      *gomock.Controller
		mockDomainCache *cache.MockDomainCache

		validator *decisionAttrValidator

		testDomainID       string
		testTargetDomainID string
	}
)

func TestDecisionAttrValidatorSuite(t *testing.T) {
	s := new(decisionAttrValidatorSuite)
	suite.Run(t, s)
}

func (s *decisionAttrValidatorSuite) SetupSuite() {
	s.testDomainID = "test domain ID"
	s.testTargetDomainID = "test target domain ID"
}

func (s *decisionAttrValidatorSuite) TearDownSuite() {
}

func (s *decisionAttrValidatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockDomainCache = cache.NewMockDomainCache(s.controller)
	config := &Config{
		MaxIDLengthLimit:                  dynamicconfig.GetIntPropertyFn(1000),
		ValidSearchAttributes:             dynamicconfig.GetMapPropertyFn(definition.GetDefaultIndexedKeys()),
		SearchAttributesNumberOfKeysLimit: dynamicconfig.GetIntPropertyFilteredByDomain(100),
		SearchAttributesSizeOfValueLimit:  dynamicconfig.GetIntPropertyFilteredByDomain(2 * 1024),
		SearchAttributesTotalSizeLimit:    dynamicconfig.GetIntPropertyFilteredByDomain(40 * 1024),
	}
	s.validator = newDecisionAttrValidator(
		s.mockDomainCache,
		config,
		log.NewNoop(),
	)
}

func (s *decisionAttrValidatorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *decisionAttrValidatorSuite) TestValidateSignalExternalWorkflowExecutionAttributes() {
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).AnyTimes()

	var attributes *commonproto.SignalExternalWorkflowExecutionDecisionAttributes

	err := s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.EqualError(err, "SignalExternalWorkflowExecutionDecisionAttributes is not set on decision.")

	attributes = &commonproto.SignalExternalWorkflowExecutionDecisionAttributes{}
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.EqualError(err, "Execution is nil on decision.")

	attributes.Execution = &commonproto.WorkflowExecution{}
	attributes.Execution.WorkflowId = "workflow-id"
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.EqualError(err, "SignalName is not set on decision.")

	attributes.Execution.RunId = "run-id"
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.EqualError(err, "Invalid RunId set on decision.")
	attributes.Execution.RunId = testRunID

	attributes.SignalName = "my signal name"
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.NoError(err)

	attributes.Input = []byte("test input")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.NoError(err)
}

func (s *decisionAttrValidatorSuite) TestValidateUpsertWorkflowSearchAttributes() {
	domainName := "testDomain"
	var attributes *commonproto.UpsertWorkflowSearchAttributesDecisionAttributes

	err := s.validator.validateUpsertWorkflowSearchAttributes(domainName, attributes)
	s.EqualError(err, "UpsertWorkflowSearchAttributesDecisionAttributes is not set on decision.")

	attributes = &commonproto.UpsertWorkflowSearchAttributesDecisionAttributes{}
	err = s.validator.validateUpsertWorkflowSearchAttributes(domainName, attributes)
	s.EqualError(err, "SearchAttributes is not set on decision.")

	attributes.SearchAttributes = &commonproto.SearchAttributes{}
	err = s.validator.validateUpsertWorkflowSearchAttributes(domainName, attributes)
	s.EqualError(err, "IndexedFields is empty on decision.")

	attributes.SearchAttributes.IndexedFields = map[string][]byte{"CustomKeywordField": []byte(`bytes`)}
	err = s.validator.validateUpsertWorkflowSearchAttributes(domainName, attributes)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_LocalToLocal() {
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_LocalToEffectiveLocal_SameCluster() {
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_LocalToEffectiveLocal_DiffCluster() {
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestAlternativeClusterName}},
		},
		1234,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_LocalToGlobal() {
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToLocal_SameCluster() {
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToLocal_DiffCluster() {
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestAlternativeClusterName}},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToEffectiveLocal_SameCluster() {
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		5678,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToEffectiveLocal_DiffCluster() {
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestAlternativeClusterName}},
		},
		5678,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToGlobal() {
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
			},
		},
		5678,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToLocal() {
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToEffectiveLocal() {
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		5678,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
			},
		},
		1234,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToGlobal_DiffDomain() {
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestAlternativeClusterName},
				{ClusterName: cluster.TestCurrentClusterName},
			},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: s.testTargetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.testDomainID).Return(domainEntry, nil).Times(1)
	s.mockDomainCache.EXPECT().GetDomainByID(s.testTargetDomainID).Return(targetDomainEntry, nil).Times(1)

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToGlobal_SameDomain() {
	targetDomainID := s.testDomainID

	err := s.validator.validateCrossDomainCall(s.testDomainID, targetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateTaskListName() {
	taskList := func(name string) *commonproto.TaskList {
		return &commonproto.TaskList{Name: name, Kind: enums.TaskListKindNormal}
	}

	testCases := []struct {
		defaultVal  string
		input       *commonproto.TaskList
		output      *commonproto.TaskList
		isOutputErr bool
	}{
		{"tl-1", nil, &commonproto.TaskList{Name: "tl-1"}, false},
		{"", taskList("tl-1"), taskList("tl-1"), false},
		{"tl-1", taskList("tl-1"), taskList("tl-1"), false},
		{"", taskList("/tl-1"), taskList("/tl-1"), false},
		{"", taskList("/__temporal_sys"), taskList("/__temporal_sys"), false},
		{"", nil, &commonproto.TaskList{}, true},
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
