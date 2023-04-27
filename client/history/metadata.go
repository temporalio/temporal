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
	clusterShardMD metadata.MD,
) (_ ClusterShardID, _ ClusterShardID, _ error) {
	var clientClusterShardID ClusterShardID
	var serverClusterShardID ClusterShardID

	clientClusterID, err := parseInt32(clusterShardMD, MetadataKeyClientClusterID)
	if err != nil {
		return clientClusterShardID, serverClusterShardID, err
	}
	clientShardID, err := parseInt32(clusterShardMD, MetadataKeyClientShardID)
	if err != nil {
		return clientClusterShardID, serverClusterShardID, err
	}
	clientClusterShardID.ClusterID = clientClusterID
	clientClusterShardID.ShardID = clientShardID

	serverClusterID, err := parseInt32(clusterShardMD, MetadataKeyServerClusterID)
	if err != nil {
		return clientClusterShardID, serverClusterShardID, err
	}
	serverShardID, err := parseInt32(clusterShardMD, MetadataKeyServerShardID)
	if err != nil {
		return clientClusterShardID, serverClusterShardID, err
	}
	serverClusterShardID.ClusterID = serverClusterID
	serverClusterShardID.ShardID = serverShardID

	return clientClusterShardID, serverClusterShardID, nil
}

func parseInt32(
	clusterShardMD metadata.MD,
	metadataKey string,
) (int32, error) {
	metadataValues := clusterShardMD.Get(metadataKey)
	if len(metadataValues) != 1 {
		return 0, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"unable to parse metadata key %v: %v",
			metadataKey,
			metadataValues,
		))
	}
	metadataValue, err := strconv.Atoi(metadataValues[0])
	if err != nil {
		return 0, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"unable to parse metadata key %v: %v",
			metadataKey,
			metadataValues,
		))
	}
	return int32(metadataValue), nil
}
