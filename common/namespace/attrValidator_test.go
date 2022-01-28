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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
)

type (
	attrValidatorSuite struct {
		suite.Suite

		controller          *gomock.Controller
		mockClusterMetadata *cluster.MockMetadata

		validator *AttrValidatorImpl
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
	s.controller = gomock.NewController(s.T())
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)

	s.validator = newAttrValidator(s.mockClusterMetadata)
}

func (s *attrValidatorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *attrValidatorSuite) TestClusterName() {
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(
		cluster.TestAllClusterInfo,
	).AnyTimes()

	err := s.validator.validateClusterName("some random foo bar")
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateClusterName(cluster.TestCurrentClusterName)
	s.NoError(err)

	err = s.validator.validateClusterName(cluster.TestAlternativeClusterName)
	s.NoError(err)
}

func (s *attrValidatorSuite) TestValidateNamespaceReplicationConfigForLocalNamespace() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(
		cluster.TestCurrentClusterName,
	).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(
		cluster.TestAllClusterInfo,
	).AnyTimes()

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
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(
		cluster.TestCurrentClusterName,
	).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(
		cluster.TestAllClusterInfo,
	).AnyTimes()

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
