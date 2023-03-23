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
	MetadataKeySourceClusterName = "temporal-source-cluster-name"
	MetadataKeySourceShardID     = "temporal-source-shard-id"
	MetadataKeyTargetClusterName = "temporal-target-cluster-name"
	MetadataKeyTargetShardID     = "temporal-target-shard-id"
)

type (
	ClusterShardID struct {
		ClusterName string
		ShardID     int32
	}
)

func EncodeClusterShardMD(
	sourceClusterShardID ClusterShardID,
	targetClusterShardID ClusterShardID,
) metadata.MD {
	return metadata.Pairs(
		MetadataKeySourceClusterName, sourceClusterShardID.ClusterName,
		MetadataKeySourceShardID, strconv.Itoa(int(sourceClusterShardID.ShardID)),
		MetadataKeyTargetClusterName, targetClusterShardID.ClusterName,
		MetadataKeyTargetShardID, strconv.Itoa(int(targetClusterShardID.ShardID)),
	)
}

func DecodeClusterShardMD(
	clusterShardMD metadata.MD,
) (_ ClusterShardID, _ ClusterShardID, _ error) {
	var sourceClusterShardID ClusterShardID
	var targetClusterShardID ClusterShardID

	clusterNames := clusterShardMD.Get(MetadataKeySourceClusterName)
	if len(clusterNames) != 1 {
		return sourceClusterShardID, targetClusterShardID, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"unable to parse source cluster shard ID: %v",
			clusterShardMD,
		))
	}
	sourceClusterShardID.ClusterName = clusterNames[0]
	shardIDs := clusterShardMD.Get(MetadataKeySourceShardID)
	if len(shardIDs) != 1 {
		return sourceClusterShardID, targetClusterShardID, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"unable to parse source cluster shard ID: %v",
			clusterShardMD,
		))
	}
	shardID, err := strconv.Atoi(shardIDs[0])
	if err != nil {
		return sourceClusterShardID, targetClusterShardID, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"unable to parse source cluster shard ID: %v, err: %v",
			clusterShardMD,
			err.Error(),
		))
	}
	sourceClusterShardID.ShardID = int32(shardID)
	clusterNames = clusterShardMD.Get(MetadataKeyTargetClusterName)
	if len(clusterNames) != 1 {
		return sourceClusterShardID, targetClusterShardID, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"unable to parse target cluster shard ID: %v",
			clusterShardMD,
		))
	}
	targetClusterShardID.ClusterName = clusterNames[0]
	shardIDs = clusterShardMD.Get(MetadataKeyTargetShardID)
	if len(shardIDs) != 1 {
		return sourceClusterShardID, targetClusterShardID, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"unable to parse source cluster shard ID: %v",
			clusterShardMD,
		))
	}
	shardID, err = strconv.Atoi(shardIDs[0])
	if err != nil {
		return sourceClusterShardID, targetClusterShardID, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"unable to parse source cluster shard ID: %v, err: %v",
			clusterShardMD,
			err.Error(),
		))
	}
	targetClusterShardID.ShardID = int32(shardID)
	return sourceClusterShardID, targetClusterShardID, nil
}
