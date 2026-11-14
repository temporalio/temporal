package persistence_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	mockp "go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
)

func TestShardManager_GetOrCreateShard_RequiresLifecycleContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mockp.NewMockShardStore(ctrl)
	manager := p.NewShardManager(store, serialization.NewSerializer())

	_, err := manager.GetOrCreateShard(context.Background(), &p.GetOrCreateShardRequest{
		ShardID: 1,
	})
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
}

func TestShardManager_GetShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mockp.NewMockShardStore(ctrl)
	serializer := serialization.NewSerializer()
	manager := p.NewShardManager(store, serializer)

	shardInfo := &persistencespb.ShardInfo{
		ShardId: 1,
		RangeId: 5,
		Owner:   "owner",
	}
	blob, err := serializer.ShardInfoToBlob(shardInfo)
	require.NoError(t, err)
	store.EXPECT().GetShard(gomock.Any(), &p.GetShardRequest{ShardID: 1}).Return(&p.InternalGetShardResponse{
		ShardInfo: blob,
	}, nil)

	resp, err := manager.GetShard(context.Background(), &p.GetShardRequest{ShardID: 1})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, shardInfo, resp.ShardInfo)
}

func TestShardManager_GetShard_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mockp.NewMockShardStore(ctrl)
	manager := p.NewShardManager(store, serialization.NewSerializer())

	store.EXPECT().GetShard(gomock.Any(), &p.GetShardRequest{ShardID: 1}).Return(nil, serviceerror.NewNotFound("shard not found"))

	_, err := manager.GetShard(context.Background(), &p.GetShardRequest{ShardID: 1})
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}
