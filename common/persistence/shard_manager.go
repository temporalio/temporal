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

package persistence

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

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
		data, err := m.serializer.ShardInfoToBlob(shardInfo, enumspb.ENCODING_TYPE_PROTO3)
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
	shardInfo, err := m.serializer.ShardInfoFromBlob(internalResp.ShardInfo, m.shardStore.GetClusterName())
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

	shardInfoBlob, err := m.serializer.ShardInfoToBlob(shardInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}
	internalRequest := &InternalUpdateShardRequest{
		ShardID:         request.ShardInfo.GetShardId(),
		RangeID:         request.ShardInfo.GetRangeId(),
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
