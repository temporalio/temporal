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

package namespace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/primitives/timestamp"
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
		retentionPeriod *time.Duration
		expectedErr     error
	}{
		{
			retentionPeriod: timestamp.DurationFromDays(10),
			expectedErr:     nil,
		},
		{
			retentionPeriod: timestamp.DurationFromDays(0),
			expectedErr:     errInvalidRetentionPeriod,
		},
		{
			retentionPeriod: timestamp.DurationFromDays(-3),
			expectedErr:     errInvalidRetentionPeriod,
		},
	}
	for _, tc := range testCases {
		actualErr := s.validator.validateNamespaceConfig(
			&persistencespb.NamespaceConfig{Retention: tc.retentionPeriod},
		)
		s.Equal(tc.expectedErr, actualErr)
	}
}

func (s *attrValidatorSuite) TestClusterName() {
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(
		cluster.TestAllClusterInfo,
	)

	err := s.validator.validateClusterName("some random foo bar")
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateClusterName(cluster.TestCurrentClusterName)
	s.NoError(err)

	err = s.validator.validateClusterName(cluster.TestAlternativeClusterName)
	s.NoError(err)
}

func (s *attrValidatorSuite) TestValidateNamespaceReplicationConfigForLocalNamespace() {
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(
		cluster.TestCurrentClusterName,
	)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(
		cluster.TestAllClusterInfo,
	)

	err := s.validator.validateNamespaceReplicationConfigForLocalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateNamespaceReplicationConfigForLocalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateNamespaceReplicationConfigForLocalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateNamespaceReplicationConfigForLocalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
			},
		},
	)
	s.NoError(err)
}

func (s *attrValidatorSuite) TestValidateNamespaceReplicationConfigForGlobalNamespace() {
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(
		cluster.TestCurrentClusterName,
	)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(
		cluster.TestAllClusterInfo,
	)

	err := s.validator.validateNamespaceReplicationConfigForGlobalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
			},
		},
	)
	s.NoError(err)

	err = s.validator.validateNamespaceReplicationConfigForGlobalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.NoError(err)

	err = s.validator.validateNamespaceReplicationConfigForGlobalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.NoError(err)

	err = s.validator.validateNamespaceReplicationConfigForGlobalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.NoError(err)
}

func (s *attrValidatorSuite) TestValidateNamespaceReplicationConfigClustersDoesNotRemove() {
	err := s.validator.validateNamespaceReplicationConfigClustersDoesNotRemove(
		[]string{
			cluster.TestCurrentClusterName,
			cluster.TestAlternativeClusterName,
		},
		[]string{
			cluster.TestCurrentClusterName,
			cluster.TestAlternativeClusterName,
		},
	)
	s.NoError(err)

	err = s.validator.validateNamespaceReplicationConfigClustersDoesNotRemove(
		[]string{
			cluster.TestCurrentClusterName,
		},
		[]string{
			cluster.TestCurrentClusterName,
			cluster.TestAlternativeClusterName,
		},
	)
	s.NoError(err)

	err = s.validator.validateNamespaceReplicationConfigClustersDoesNotRemove(
		[]string{
			cluster.TestCurrentClusterName,
			cluster.TestAlternativeClusterName,
		},
		[]string{
			cluster.TestAlternativeClusterName,
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateNamespaceReplicationConfigClustersDoesNotRemove(
		[]string{
			cluster.TestCurrentClusterName,
		},
		[]string{
			cluster.TestAlternativeClusterName,
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}
