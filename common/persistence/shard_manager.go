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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
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

func (m *shardManagerImpl) GetOrCreateShard(request *GetOrCreateShardRequest) (*GetShardResponse, error) {
	internalResp, err := m.shardStore.GetShard(&InternalGetShardRequest{
		ShardID: request.ShardID,
	})
	if err == nil {
		shardInfo, err := m.serializer.ShardInfoFromBlob(internalResp.ShardInfo, m.shardStore.GetClusterName())
		if err != nil {
			return nil, err
		}
		return &GetShardResponse{
			ShardInfo: shardInfo,
		}, nil
	}

	if _, ok := err.(*serviceerror.NotFound); !ok || !request.CreateIfMissing {
		return nil, err
	}

	// Not in shard store: create it. Fill in initial shard info if missing.
	shardInfo := request.InitialShardInfo
	if shardInfo == nil {
		shardInfo = &persistencespb.ShardInfo{}
	}
	shardInfo.ShardId = request.ShardID
	shardInfo.UpdateTime = timestamp.TimeNowPtrUtc()
	data, err := m.serializer.ShardInfoToBlob(shardInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}
	internalRequest := &InternalCreateShardRequest{
		ShardID:   shardInfo.GetShardId(),
		RangeID:   shardInfo.GetRangeId(),
		ShardInfo: data,
	}
	err = m.shardStore.CreateShard(internalRequest)
	if _, ok := err.(*ShardAlreadyExistError); ok {
		// raced with someone else trying to create it; just Get again
		request.CreateIfMissing = false // defensive just to avoid loop if shardStore is broken
		return m.GetOrCreateShard(request)
	}
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
		ShardID:         request.ShardInfo.GetShardId(),
		RangeID:         request.ShardInfo.GetRangeId(),
		ShardInfo:       shardInfoBlob,
		PreviousRangeID: request.PreviousRangeID,
	}
	return m.shardStore.UpdateShard(internalRequest)
}
