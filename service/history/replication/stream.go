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

package replication

import (
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/cluster"
)

type (
	Stream          BiDirectionStream[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse]
	ClusterShardKey struct {
		ClusterID int32
		ShardID   int32
	}
	ClusterShardKeyPair struct {
		Client ClusterShardKey
		Server ClusterShardKey
	}
)

func ClusterIDToClusterNameShardCount(
	allClusterInfo map[string]cluster.ClusterInformation,
	clusterID int32,
) (string, int32, error) {
	for clusterName, clusterInfo := range allClusterInfo {
		if int32(clusterInfo.InitialFailoverVersion) == clusterID {
			return clusterName, clusterInfo.ShardCount, nil
		}
	}
	return "", 0, serviceerror.NewInternal(fmt.Sprintf("unknown cluster ID: %v", clusterID))
}

func WrapEventLoop(
	originalEventLoop func() error,
	streamStopper func(),
	logger log.Logger,
	metricsHandler metrics.Handler,
	fromClusterKey ClusterShardKey,
	toClusterKey ClusterShardKey,
	retryInterval time.Duration,
) {
	defer streamStopper()
	for {
		err := originalEventLoop()

		if err == nil {
			return
		}
		// if it is stream error, we will not retry and terminate the stream, then let the stream_receiver_monitor to restart it
		if streamError, ok := err.(*StreamError); ok {
			metricsHandler.Counter(metrics.ReplicationStreamError.Name()).Record(
				int64(1),
				metrics.ServiceErrorTypeTag(streamError.cause),
				metrics.FromClusterIDTag(fromClusterKey.ClusterID),
				metrics.ToClusterIDTag(toClusterKey.ClusterID),
			)
			return
		}
		time.Sleep(retryInterval)
	}
}
