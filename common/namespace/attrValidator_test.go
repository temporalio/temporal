package namespace

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
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
		actualErr := s.validator.validateNamespaceConfig(
			&persistence.NamespaceConfig{Retention: tc.retentionPeriod},
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
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateNamespaceReplicationConfigForLocalNamespace(
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateNamespaceReplicationConfigForLocalNamespace(
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateNamespaceReplicationConfigForLocalNamespace(
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
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
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
			},
		},
	)
	s.NoError(err)

	err = s.validator.validateNamespaceReplicationConfigForGlobalNamespace(
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.NoError(err)

	err = s.validator.validateNamespaceReplicationConfigForGlobalNamespace(
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.NoError(err)

	err = s.validator.validateNamespaceReplicationConfigForGlobalNamespace(
		&persistence.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
	)
	s.NoError(err)
}

func (s *attrValidatorSuite) TestValidateNamespaceReplicationConfigClustersDoesNotRemove() {
	err := s.validator.validateNamespaceReplicationConfigClustersDoesNotRemove(
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

	err = s.validator.validateNamespaceReplicationConfigClustersDoesNotRemove(
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
		},
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	)
	s.NoError(err)

	err = s.validator.validateNamespaceReplicationConfigClustersDoesNotRemove(
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	err = s.validator.validateNamespaceReplicationConfigClustersDoesNotRemove(
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
		},
		[]*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}
