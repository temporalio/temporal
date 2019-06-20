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
	}
)

func TestDecisionAttrValidatorSuite(t *testing.T) {
	s := new(decisionAttrValidatorSuite)
	suite.Run(t, s)
}

func (s *decisionAttrValidatorSuite) SetupSuite() {
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

}

func (s *decisionAttrValidatorSuite) TestValidateSignalExternalWorkflowExecutionAttributes() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: targetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	var attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes

	err := s.validator.validateSignalExternalWorkflowExecutionAttributes(domainID, targetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: SignalExternalWorkflowExecutionDecisionAttributes is not set on decision.}")

	attributes = &workflow.SignalExternalWorkflowExecutionDecisionAttributes{}
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(domainID, targetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: Execution is nil on decision.}")

	attributes.Execution = &workflow.WorkflowExecution{}
	attributes.Execution.WorkflowId = common.StringPtr("workflow-id")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(domainID, targetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: SignalName is not set on decision.}")

	attributes.Execution.RunId = common.StringPtr("run-id")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(domainID, targetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: Invalid RunId set on decision.}")
	attributes.Execution.RunId = common.StringPtr(validRunID)

	attributes.SignalName = common.StringPtr("my signal name")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(domainID, targetDomainID, attributes)
	s.EqualError(err, "BadRequestError{Message: Input is not set on decision.}")

	attributes.Input = []byte("test input")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(domainID, targetDomainID, attributes)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_LocalToLocal() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: targetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_LocalToEffectiveLocal_SameCluster() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: targetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_LocalToEffectiveLocal_DiffCluster() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: targetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestAlternativeClusterName}},
		},
		1234,
		nil,
	)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_LocalToGlobal() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: targetDomainID},
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

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToLocal_SameCluster() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: targetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToLocal_DiffCluster() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestAlternativeClusterName}},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: targetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToEffectiveLocal_SameCluster() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: targetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		5678,
		nil,
	)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.Nil(err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToEffectiveLocal_DiffCluster() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
		},
		1234,
		nil,
	)
	targetDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: targetDomainID},
		nil,
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestAlternativeClusterName}},
		},
		5678,
		nil,
	)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_EffectiveLocalToGlobal() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
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
		&persistence.DomainInfo{Name: targetDomainID},
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

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToLocal() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
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
		&persistence.DomainInfo{Name: targetDomainID},
		nil,
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToEffectiveLocal() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
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
		&persistence.DomainInfo{Name: targetDomainID},
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

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToGlobal_DiffDomain() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: domainID},
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
		&persistence.DomainInfo{Name: targetDomainID},
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

	s.mockDomainCache.On("GetDomainByID", domainID).Return(domainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", targetDomainID).Return(targetDomainEntry, nil)

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *decisionAttrValidatorSuite) TestValidateCrossDomainCall_GlobalToGlobal_SameDomain() {
	domainID := "some random domain ID"
	targetDomainID := domainID

	err := s.validator.validateCrossDomainCall(domainID, targetDomainID)
	s.Nil(err)
}
