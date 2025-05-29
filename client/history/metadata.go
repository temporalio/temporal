package history

import (
	"strconv"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/headers"
	"google.golang.org/grpc/metadata"
)

const (
	MetadataKeyClientClusterID = "temporal-client-cluster-id"
	MetadataKeyClientShardID   = "temporal-client-shard-id"
	MetadataKeyServerClusterID = "temporal-server-cluster-id"
	MetadataKeyServerShardID   = "temporal-server-shard-id"
)

type (
	ClusterShardID struct {
		ClusterID int32
		ShardID   int32
	}
)

func EncodeClusterShardMD(
	sourceClusterShardID ClusterShardID,
	targetClusterShardID ClusterShardID,
) metadata.MD {
	return metadata.Pairs(
		MetadataKeyClientClusterID, strconv.Itoa(int(sourceClusterShardID.ClusterID)),
		MetadataKeyClientShardID, strconv.Itoa(int(sourceClusterShardID.ShardID)),
		MetadataKeyServerClusterID, strconv.Itoa(int(targetClusterShardID.ClusterID)),
		MetadataKeyServerShardID, strconv.Itoa(int(targetClusterShardID.ShardID)),
	)
}

func DecodeClusterShardMD(
	getter headers.HeaderGetter,
) (client ClusterShardID, server ClusterShardID, err error) {
	if client.ClusterID, err = parseInt32(getter, MetadataKeyClientClusterID); err != nil {
		return
	} else if client.ShardID, err = parseInt32(getter, MetadataKeyClientShardID); err != nil {
		return
	} else if server.ClusterID, err = parseInt32(getter, MetadataKeyServerClusterID); err != nil {
		return
	} else if server.ShardID, err = parseInt32(getter, MetadataKeyServerShardID); err != nil {
		return
	}
	return
}

func parseInt32(
	getter headers.HeaderGetter,
	metadataKey string,
) (int32, error) {
	stringValue := getter.Get(metadataKey)
	if stringValue == "" {
		return 0, serviceerror.NewInvalidArgument("missing cluster & shard ID metadata")
	}
	metadataValue, err := strconv.Atoi(stringValue)
	if err != nil {
		return 0, serviceerror.NewInvalidArgumentf(
			"unable to parse metadata key %v: %v",
			metadataKey,
			err,
		)
	}
	return int32(metadataValue), nil
}
