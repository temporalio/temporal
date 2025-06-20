package nsmanager

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.uber.org/mock/gomock"
)

type (
	attrValidatorSuite struct {
		suite.Suite

		controller          *gomock.Controller
		mockClusterMetadata *cluster.MockMetadata

		validator *Validator
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

	s.validator = NewValidator(s.mockClusterMetadata)
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

	err := s.validator.ValidateNamespaceReplicationConfigForLocalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.ValidateNamespaceReplicationConfigForLocalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.ValidateNamespaceReplicationConfigForLocalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.ValidateNamespaceReplicationConfigForLocalNamespace(
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

	err := s.validator.ValidateNamespaceReplicationConfigForGlobalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
			},
		},
	)
	s.NoError(err)

	err = s.validator.ValidateNamespaceReplicationConfigForGlobalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.NoError(err)

	err = s.validator.ValidateNamespaceReplicationConfigForGlobalNamespace(
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	)
	s.NoError(err)

	err = s.validator.ValidateNamespaceReplicationConfigForGlobalNamespace(
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
