package history

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/headers"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"
)

type (
	metadataSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}
)

func TestMetadataSuite(t *testing.T) {
	s := new(metadataSuite)
	suite.Run(t, s)
}

func (s *metadataSuite) SetupSuite() {
}

func (s *metadataSuite) TearDownSuite() {
}

func (s *metadataSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

}

func (s *metadataSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *metadataSuite) TestClusterShardMD_Encode_Decode() {
	clientClusterShardID := ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	serverClusterShardID := ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}

	clusterShardMD := EncodeClusterShardMD(
		clientClusterShardID,
		serverClusterShardID,
	)
	ctx := metadata.NewIncomingContext(context.Background(), clusterShardMD)
	getter := headers.NewGRPCHeaderGetter(ctx)
	actualClientClusterShardID, actualServerClusterShardID, err := DecodeClusterShardMD(getter)
	s.NoError(err)
	s.Equal(clientClusterShardID, actualClientClusterShardID)
	s.Equal(serverClusterShardID, actualServerClusterShardID)
}

func (s *metadataSuite) TestClusterShardMD_Decode_Error() {
	clusterShardMD := metadata.Pairs(
		MetadataKeyClientShardID, strconv.Itoa(int(rand.Int31())),
		MetadataKeyServerClusterID, uuid.NewString(),
		MetadataKeyServerShardID, strconv.Itoa(int(rand.Int31())),
	)
	ctx := metadata.NewIncomingContext(context.Background(), clusterShardMD)
	getter := headers.NewGRPCHeaderGetter(ctx)
	_, _, err := DecodeClusterShardMD(getter)
	s.Error(err)

	clusterShardMD = metadata.Pairs(
		MetadataKeyClientClusterID, uuid.NewString(),
		MetadataKeyServerClusterID, uuid.NewString(),
		MetadataKeyServerShardID, strconv.Itoa(int(rand.Int31())),
	)
	ctx = metadata.NewIncomingContext(context.Background(), clusterShardMD)
	getter = headers.NewGRPCHeaderGetter(ctx)
	_, _, err = DecodeClusterShardMD(getter)
	s.Error(err)

	clusterShardMD = metadata.Pairs(
		MetadataKeyClientClusterID, uuid.NewString(),
		MetadataKeyClientShardID, strconv.Itoa(int(rand.Int31())),
		MetadataKeyServerShardID, strconv.Itoa(int(rand.Int31())),
	)
	ctx = metadata.NewIncomingContext(context.Background(), clusterShardMD)
	getter = headers.NewGRPCHeaderGetter(ctx)
	_, _, err = DecodeClusterShardMD(getter)
	s.Error(err)

	clusterShardMD = metadata.Pairs(
		MetadataKeyClientClusterID, uuid.NewString(),
		MetadataKeyClientShardID, strconv.Itoa(int(rand.Int31())),
		MetadataKeyServerClusterID, uuid.NewString(),
	)
	ctx = metadata.NewIncomingContext(context.Background(), clusterShardMD)
	getter = headers.NewGRPCHeaderGetter(ctx)
	_, _, err = DecodeClusterShardMD(getter)
	s.Error(err)

	clusterShardMD = metadata.Pairs(
		MetadataKeyClientClusterID, uuid.NewString(),
		MetadataKeyClientShardID, uuid.NewString(),
		MetadataKeyServerClusterID, uuid.NewString(),
		MetadataKeyServerShardID, uuid.NewString(),
	)
	ctx = metadata.NewIncomingContext(context.Background(), clusterShardMD)
	getter = headers.NewGRPCHeaderGetter(ctx)
	_, _, err = DecodeClusterShardMD(getter)
	s.Error(err)
}
