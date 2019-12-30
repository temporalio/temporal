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

package domain

import (
	"testing"

	"github.com/uber/cadence/.gen/go/shared"

	"github.com/uber/cadence/common/cluster"

	"github.com/uber/cadence/common/persistence"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/mocks"
)

type (
	attrValidatorSuite struct {
		suite.Suite

		minRetentionDays    int
		mockClusterMetadata *mocks.ClusterMetadata
		validator           *AttrValidatorImpl
	}
)

func TestAttrValidatorSuite(t *testing.T) {
	s := new(attrValidatorSuite)
	suite.Run(t, s)
}

func (s *attrValidatorSuite) SetupSuite() {
}

func (s *attrValidatorSuite) TearDownSuite() {
}

func (s *attrValidatorSuite) SetupTest() {
	s.minRetentionDays = 1
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.validator = newAttrValidator(s.mockClusterMetadata, int32(s.minRetentionDays))
}

func (s *attrValidatorSuite) TearDownTest() {
}

func (s *attrValidatorSuite) TestValidateConfigRetentionPeriod() {
	testCases := []struct {
		retentionPeriod int32
		expectedErr     error
	}{
		{
			retentionPeriod: 10,
			expectedErr:     nil,
		},
		{
			retentionPeriod: 0,
			expectedErr:     errInvalidRetentionPeriod,
		},
		{
			retentionPeriod: -3,
			expectedErr:     errInvalidRetentionPeriod,
		},
	}
	for _, tc := range testCases {
		actualErr := s.validator.validateDomainConfig(
			&persistence.DomainConfig{Retention: tc.retentionPeriod},
		)
		s.Equal(tc.expectedErr, actualErr)
	}
}

func (s *attrValidatorSuite) TestClusterName() {
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(
		cluster.TestAllClusterInfo,
	)

	err := s.validator.validateClusterName("some random foo bar")
	s.IsType(&shared.BadRequestError{}, err)

	err = s.validator.validateClusterName(cluster.TestCurrentClusterName)
	s.NoError(err)

	err = s.validator.validateClusterName(cluster.TestAlternativeClusterName)
	s.NoError(err)
}

func (s *attrValidatorSuite) TestValidateDomainReplicationConfigForLocalDomain() {
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(
		cluster.TestCurrentClusterName,
	)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(
		cluster.TestAllClusterInfo,
	)

	err := s.validator.validateDomainReplicationConfigForLocalDomain(
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.IsType(&shared.BadRequestError{}, err)

	err = s.validator.validateDomainReplicationConfigForLocalDomain(
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.IsType(&shared.BadRequestError{}, err)

	err = s.validator.validateDomainReplicationConfigForLocalDomain(
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.IsType(&shared.BadRequestError{}, err)

	err = s.validator.validateDomainReplicationConfigForLocalDomain(
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
			},
		},
	)
	s.NoError(err)
}

func (s *attrValidatorSuite) TestValidateDomainReplicationConfigForGlobalDomain() {
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(
		cluster.TestCurrentClusterName,
	)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(
		cluster.TestAllClusterInfo,
	)

	err := s.validator.validateDomainReplicationConfigForGlobalDomain(
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
			},
		},
	)
	s.NoError(err)

	err = s.validator.validateDomainReplicationConfigForGlobalDomain(
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.NoError(err)

	err = s.validator.validateDomainReplicationConfigForGlobalDomain(
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.NoError(err)

	err = s.validator.validateDomainReplicationConfigForGlobalDomain(
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.NoError(err)
}

func (s *attrValidatorSuite) TestValidateDomainReplicationConfigClustersDoesNotRemove() {
	err := s.validator.validateDomainReplicationConfigClustersDoesNotRemove(
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	)
	s.NoError(err)

	err = s.validator.validateDomainReplicationConfigClustersDoesNotRemove(
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
		},
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	)
	s.NoError(err)

	err = s.validator.validateDomainReplicationConfigClustersDoesNotRemove(
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	)
	s.IsType(&shared.BadRequestError{}, err)

	err = s.validator.validateDomainReplicationConfigClustersDoesNotRemove(
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
		},
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	)
	s.IsType(&shared.BadRequestError{}, err)
}
