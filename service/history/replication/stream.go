package replication

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

var (
	streamRetryPolicy = backoff.NewExponentialRetryPolicy(500 * time.Millisecond).
		WithMaximumInterval(time.Second * 2)
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
	ctx context.Context,
	originalEventLoop func() error,
	streamStopper func(),
	logger log.Logger,
	metricsHandler metrics.Handler,
	fromClusterKey ClusterShardKey,
	toClusterKey ClusterShardKey,
	retryPolicy backoff.RetryPolicy,
) {
	defer streamStopper()

	ops := func() error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := originalEventLoop()

		if err != nil {
			var streamError *StreamError
			if errors.As(err, &streamError) {
				metrics.ReplicationStreamError.With(metricsHandler).Record(
					int64(1),
					metrics.ServiceErrorTypeTag(streamError.cause),
					metrics.FromClusterIDTag(fromClusterKey.ClusterID),
					metrics.ToClusterIDTag(toClusterKey.ClusterID),
				)
				logger.Warn("ReplicationStreamError", tag.Error(err))
			} else {
				metrics.ReplicationServiceError.With(metricsHandler).Record(
					int64(1),
					metrics.ServiceErrorTypeTag(err),
					metrics.FromClusterIDTag(fromClusterKey.ClusterID),
					metrics.ToClusterIDTag(toClusterKey.ClusterID),
				)
				logger.Error("ReplicationServiceError", tag.Error(err))
			}
			return err
		}
		// shutdown case
		return nil
	}
	_ = backoff.ThrottleRetry(ops, retryPolicy, isRetryableError)
}

func isRetryableError(err error) bool {
	var streamError *StreamError
	var shardOwnershipLostError *persistence.ShardOwnershipLostError
	var shardOwnershipLost *serviceerrors.ShardOwnershipLost
	switch {
	case errors.As(err, &shardOwnershipLostError):
		return false
	case errors.As(err, &shardOwnershipLost):
		return false
	case errors.As(err, &streamError):
		return false
	case errors.Is(err, context.Canceled):
		return false
	default:
		return true
	}
}
