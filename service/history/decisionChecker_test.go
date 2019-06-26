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

	"github.com/stretchr/testify/suite"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
)

type (
	decisionAttrValidatorSuite struct {
		suite.Suite

		mockDomainCache *cache.DomainCacheMock

		maxIDLengthLimit int
		validator        *decisionAttrValidator

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
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.maxIDLengthLimit = 1000
	s.validator = newDecisionAttrValidator(
		s.mockDomainCache,
		s.maxIDLengthLimit,
	)
}

func (s *decisionAttrValidatorSuite) TearDownTest() {
	s.mockDomainCache.AssertExpectations(s.T())
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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil)

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
	attributes.Execution.RunId = common.StringPtr(validRunID)

	attributes.SignalName = common.StringPtr("my signal name")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: Input is not set on decision.}")

	attributes.Input = []byte("test input")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(s.testDomainID, s.testTargetDomainID, attributes)
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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", s.testDomainID).Return(domainEntry, nil).Once()
	s.mockDomainCache.On("GetDomainByID", s.testTargetDomainID).Return(targetDomainEntry, nil).Once()

	err := s.validator.validateCrossDomainCall(s.testDomainID, s.testTargetDomainID)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToGlobal_SameDomain() {
	targetDomainID := s.testDomainID

	err := s.validator.validateCrossDomainCall(s.testDomainID, targetDomainID)
	s.Nil(err)
}
