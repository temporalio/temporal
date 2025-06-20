package shard

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	contextUtilSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestContextUtilSuite(t *testing.T) {
	s := &contextUtilSuite{}
	suite.Run(t, s)
}

func (s *contextUtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *contextUtilSuite) TeardownTest() {
	s.Assertions = require.New(s.T())
}

func (s *contextUtilSuite) TestReplicationReaderIDConversion() {
	expectedClusterID := int64(rand.Int31())
	expectedShardID := rand.Int31()

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *contextUtilSuite) TestReplicationReaderIDConversion_1() {
	expectedClusterID := int64(1)
	expectedShardID := int32(1)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *contextUtilSuite) TestReplicationReaderIDConversion_Int32Max() {
	expectedClusterID := int64(math.MaxInt32)
	expectedShardID := int32(math.MaxInt32)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *contextUtilSuite) newMockContext() *historyi.MockShardContext {
	controller := gomock.NewController(s.T())
	mockContext := historyi.NewMockShardContext(controller)
	mockContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	mockContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	return mockContext
}
