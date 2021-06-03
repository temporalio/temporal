package persistence

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

type shardManagerImpl struct {
	shardStore ShardStore
	serializer serialization.Serializer
}

// NewShardManager create a new instance of ShardManager
func NewShardManager(
	shardStore ShardStore,
) ShardManager {
	return &shardManagerImpl{
		shardStore: shardStore,
		serializer: serialization.NewSerializer(),
	}
}

func (m *shardManagerImpl) Close() {
	m.shardStore.Close()
}

func (m *shardManagerImpl) GetName() string {
	return m.shardStore.GetName()
}

func (m *shardManagerImpl) CreateShard(request *CreateShardRequest) error {
	shardInfo := request.ShardInfo
	shardInfo.UpdateTime = timestamp.TimeNowPtrUtc()
	data, err := m.serializer.ShardInfoToBlob(shardInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}

	internalRequest := &InternalCreateShardRequest{
		ShardID: shardInfo.GetShardId(),
		RangeID: shardInfo.GetRangeId(),
		ShardInfo: data,
	}
	return m.shardStore.CreateShard(internalRequest)
}

func (m *shardManagerImpl) GetShard(request *GetShardRequest) (*GetShardResponse, error) {
	internalResp, err := m.shardStore.GetShard(&InternalGetShardRequest{
		ShardID: request.ShardID,
	})
	if err != nil {
		return nil, err
	}
	shardInfo, err := m.serializer.ShardInfoFromBlob(internalResp.ShardInfo, m.shardStore.GetClusterName())
	if err != nil {
		return nil, err
	}
	return &GetShardResponse{
		ShardInfo: shardInfo,
	}, nil
}

func (m *shardManagerImpl) UpdateShard(request *UpdateShardRequest) error {
	shardInfo := request.ShardInfo
	shardInfo.UpdateTime = timestamp.TimeNowPtrUtc()

	shardInfoBlob, err := m.serializer.ShardInfoToBlob(shardInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}
	internalRequest := &InternalUpdateShardRequest{
		ShardID: request.ShardInfo.GetShardId(),
		RangeID: request.ShardInfo.GetRangeId(),
		ShardInfo: shardInfoBlob,
		PreviousRangeID: request.PreviousRangeID,
	}
	return m.shardStore.UpdateShard(internalRequest)
}
