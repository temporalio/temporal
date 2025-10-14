package persistence

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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
	serializer serialization.Serializer,
) ShardManager {
	return &shardManagerImpl{
		shardStore: shardStore,
		serializer: serializer,
	}
}

func (m *shardManagerImpl) Close() {
	m.shardStore.Close()
}

func (m *shardManagerImpl) GetName() string {
	return m.shardStore.GetName()
}

func (m *shardManagerImpl) GetOrCreateShard(
	ctx context.Context,
	request *GetOrCreateShardRequest,
) (*GetOrCreateShardResponse, error) {
	createShardInfo := func() (int64, *commonpb.DataBlob, error) {
		shardInfo := request.InitialShardInfo
		if shardInfo == nil {
			shardInfo = &persistencespb.ShardInfo{}
		}
		shardInfo.ShardId = request.ShardID
		shardInfo.UpdateTime = timestamp.TimeNowPtrUtc()
		data, err := m.serializer.ShardInfoToBlob(shardInfo)
		if err != nil {
			return 0, nil, err
		}
		return shardInfo.GetRangeId(), data, nil
	}
	internalResp, err := m.shardStore.GetOrCreateShard(ctx, &InternalGetOrCreateShardRequest{
		ShardID:          request.ShardID,
		CreateShardInfo:  createShardInfo,
		LifecycleContext: request.LifecycleContext,
	})
	if err != nil {
		return nil, err
	}
	shardInfo, err := m.serializer.ShardInfoFromBlob(internalResp.ShardInfo)
	if err != nil {
		return nil, err
	}
	return &GetOrCreateShardResponse{
		ShardInfo: shardInfo,
	}, nil
}

func (m *shardManagerImpl) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) error {
	shardInfo := request.ShardInfo
	shardInfo.UpdateTime = timestamp.TimeNowPtrUtc()

	shardInfoBlob, err := m.serializer.ShardInfoToBlob(shardInfo)
	if err != nil {
		return err
	}
	internalRequest := &InternalUpdateShardRequest{
		ShardID:         request.ShardInfo.GetShardId(),
		RangeID:         request.ShardInfo.GetRangeId(),
		Owner:           request.ShardInfo.GetOwner(),
		ShardInfo:       shardInfoBlob,
		PreviousRangeID: request.PreviousRangeID,
	}
	return m.shardStore.UpdateShard(ctx, internalRequest)
}

func (m *shardManagerImpl) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) error {
	return m.shardStore.AssertShardOwnership(ctx, request)
}
