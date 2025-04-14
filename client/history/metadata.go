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

package history

import (
	"fmt"
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
		return 0, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"unable to parse metadata key %v: %v",
			metadataKey,
			err,
		))
	}
	return int32(metadataValue), nil
}
