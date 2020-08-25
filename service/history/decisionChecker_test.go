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

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/constants"
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
	config := &config.Config{
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

	var attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes

	err := s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: SignalExternalWorkflowExecutionDecisionAttributes is not set on decision.}")

	attributes = &workflow.SignalExternalWorkflowExecutionDecisionAttributes{}
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: Execution is nil on decision.}")

	attributes.Execution = &workflow.WorkflowExecution{}
	attributes.Execution.WorkflowId = common.StringPtr("workflow-id")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: SignalName is not set on decision.}")

	attributes.Execution.RunId = common.StringPtr("run-id")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: Invalid RunId set on decision.}")
	attributes.Execution.RunId = common.StringPtr(constants.TestRunID)

	attributes.SignalName = common.StringPtr("my signal name")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.NoError(err)

	attributes.Input = []byte("test input")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.NoError(err)
}

func (s *decisionAttrValidatorSuite) TestValidateUpsertWorkflowSearchAttributes() {
	domainName := "testDomain"
	var attributes *workflow.UpsertWorkflowSearchAttributesDecisionAttributes

	err := s.validator.validateUpsertWorkflowSearchAttributes(domainName, attributes)
	s.EqualError(err, "BadRequestError{Message: UpsertWorkflowSearchAttributesDecisionAttributes is not set on decision.}")

	attributes = &workflow.UpsertWorkflowSearchAttributesDecisionAttributes{}
	err = s.validator.validateUpsertWorkflowSearchAttributes(domainName, attributes)
	s.EqualError(err, "BadRequestError{Message: SearchAttributes is not set on decision.}")

	attributes.SearchAttributes = &workflow.SearchAttributes{}
	err = s.validator.validateUpsertWorkflowSearchAttributes(domainName, attributes)
	s.EqualError(err, "BadRequestError{Message: IndexedFields is empty on decision.}")

	attributes.SearchAttributes.IndexedFields = map[string][]byte{"CustomKeywordField": []byte(`"bytes"`)}
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
	s.IsType(&workflow.BadRequestError{}, err)
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
	s.IsType(&workflow.BadRequestError{}, err)
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
	s.IsType(&workflow.BadRequestError{}, err)
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
	s.IsType(&workflow.BadRequestError{}, err)
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
	s.IsType(&workflow.BadRequestError{}, err)
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
	s.IsType(&workflow.BadRequestError{}, err)
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
	s.IsType(&workflow.BadRequestError{}, err)
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
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToGlobal_SameDomain() {
	targetDomainID := s.testDomainID

	err := s.validator.validateCrossDomainCall(s.testDomainID, targetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateTaskListName() {
	taskList := func(name string) *workflow.TaskList {
		kind := workflow.TaskListKindNormal
		return &workflow.TaskList{Name: &name, Kind: &kind}
	}

	testCases := []struct {
		defaultVal  string
		input       *workflow.TaskList
		output      *workflow.TaskList
		isOutputErr bool
	}{
		{"tl-1", nil, &workflow.TaskList{Name: common.StringPtr("tl-1")}, false},
		{"", taskList("tl-1"), taskList("tl-1"), false},
		{"tl-1", taskList("tl-1"), taskList("tl-1"), false},
		{"", taskList("/tl-1"), taskList("/tl-1"), false},
		{"", taskList("/__cadence_sys"), taskList("/__cadence_sys"), false},
		{"", nil, &workflow.TaskList{}, true},
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

func (s *decisionAttrValidatorSuite) TestValidateActivityScheduleAttributes_NoRetryPolicy() {
	wfTimeout := int32(5)
	attributes := &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId: common.StringPtr("some random activityID"),
		ActivityType: &workflow.ActivityType{
			Name: common.StringPtr("some random activity type"),
		},
		Domain: common.StringPtr(s.testDomainID),
		TaskList: &workflow.TaskList{
			Name: common.StringPtr("some random task list"),
		},
		Input:                         []byte{1, 2, 3},
		ScheduleToCloseTimeoutSeconds: nil, // not set
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(3),
		StartToCloseTimeoutSeconds:    common.Int32Ptr(3),  // ScheduleToStart + StartToClose > wfTimeout
		HeartbeatTimeoutSeconds:       common.Int32Ptr(10), // larger then wfTimeout
	}

	expectedAttributesAfterValidation := &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    attributes.ActivityId,
		ActivityType:                  attributes.ActivityType,
		Domain:                        attributes.Domain,
		TaskList:                      attributes.TaskList,
		Input:                         attributes.Input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(wfTimeout),
		ScheduleToStartTimeoutSeconds: attributes.ScheduleToStartTimeoutSeconds,
		StartToCloseTimeoutSeconds:    attributes.StartToCloseTimeoutSeconds,
		HeartbeatTimeoutSeconds:       common.Int32Ptr(wfTimeout),
	}

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

	err := s.validator.validateActivityScheduleAttributes(
		s.testDomainID,
		s.testTargetDomainID,
		attributes,
		wfTimeout,
	)
	s.Nil(err)
	s.Equal(expectedAttributesAfterValidation, attributes)
}

func (s *decisionAttrValidatorSuite) TestValidateActivityScheduleAttributes_WithRetryPolicy_ScheduleToStartRetryable() {
	wfTimeout := int32(2000)
	attributes := &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId: common.StringPtr("some random activityID"),
		ActivityType: &workflow.ActivityType{
			Name: common.StringPtr("some random activity type"),
		},
		Domain: common.StringPtr(s.testDomainID),
		TaskList: &workflow.TaskList{
			Name: common.StringPtr("some random task list"),
		},
		Input:                         []byte{1, 2, 3},
		ScheduleToCloseTimeoutSeconds: nil, // not set
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(3),
		StartToCloseTimeoutSeconds:    common.Int32Ptr(500), // extended ScheduleToStart + StartToClose > wfTimeout
		HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
		RetryPolicy: &workflow.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(1.1),
			ExpirationIntervalInSeconds: common.Int32Ptr(maximumScheduleToStartTimeoutForRetryInSeconds + 1000), // larger than wfTimeout and maximumScheduleToStartTimeoutForRetryInSeconds
			NonRetriableErrorReasons:    []string{"non-retryable error"},
		},
	}

	expectedAttributesAfterValidation := &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    attributes.ActivityId,
		ActivityType:                  attributes.ActivityType,
		Domain:                        attributes.Domain,
		TaskList:                      attributes.TaskList,
		Input:                         attributes.Input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(wfTimeout),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(maximumScheduleToStartTimeoutForRetryInSeconds),
		StartToCloseTimeoutSeconds:    attributes.StartToCloseTimeoutSeconds,
		HeartbeatTimeoutSeconds:       attributes.HeartbeatTimeoutSeconds,
		RetryPolicy:                   attributes.RetryPolicy,
	}

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

	err := s.validator.validateActivityScheduleAttributes(
		s.testDomainID,
		s.testTargetDomainID,
		attributes,
		wfTimeout,
	)
	s.Nil(err)
	s.Equal(expectedAttributesAfterValidation, attributes)
}

func (s *decisionAttrValidatorSuite) TestValidateActivityScheduleAttributes_WithRetryPolicy_ScheduleToStartNonRetryable() {
	wfTimeout := int32(1000)
	attributes := &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId: common.StringPtr("some random activityID"),
		ActivityType: &workflow.ActivityType{
			Name: common.StringPtr("some random activity type"),
		},
		Domain: common.StringPtr(s.testDomainID),
		TaskList: &workflow.TaskList{
			Name: common.StringPtr("some random task list"),
		},
		Input:                         []byte{1, 2, 3},
		ScheduleToCloseTimeoutSeconds: nil, // not set
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(3),
		StartToCloseTimeoutSeconds:    common.Int32Ptr(500), // extended ScheduleToStart + StartToClose > wfTimeout
		HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
		RetryPolicy: &workflow.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(1.1),
			ExpirationIntervalInSeconds: common.Int32Ptr(maximumScheduleToStartTimeoutForRetryInSeconds + 1000), // larger than wfTimeout and maximumScheduleToStartTimeoutForRetryInSeconds
			NonRetriableErrorReasons:    []string{"cadenceInternal:Timeout SCHEDULE_TO_START"},
		},
	}

	expectedAttributesAfterValidation := &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    attributes.ActivityId,
		ActivityType:                  attributes.ActivityType,
		Domain:                        attributes.Domain,
		TaskList:                      attributes.TaskList,
		Input:                         attributes.Input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(503),
		ScheduleToStartTimeoutSeconds: attributes.ScheduleToStartTimeoutSeconds,
		StartToCloseTimeoutSeconds:    attributes.StartToCloseTimeoutSeconds,
		HeartbeatTimeoutSeconds:       attributes.HeartbeatTimeoutSeconds,
		RetryPolicy:                   attributes.RetryPolicy,
	}

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

	err := s.validator.validateActivityScheduleAttributes(
		s.testDomainID,
		s.testTargetDomainID,
		attributes,
		wfTimeout,
	)
	s.Nil(err)
	s.Equal(expectedAttributesAfterValidation, attributes)
}
