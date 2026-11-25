package matching

import (
	"errors"
	"sync/atomic"
	"time"

	deploymentspb "go.temporal.io/server/api/deployment/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/cache"
)

var errSyncMatchStartDeadline = errors.New("sync match start deadline expired")

// This error travels on the local sync offer's response channel. Only the poll handler
// that withheld the response can create it.
type withheldActivityStartError struct {
	error
	requestID string
	worker    *pollMetadata
}

type activityStartKey struct {
	transferID string
	startID    string
}

const activityStartReceiptTTL = 30 * time.Second

func newActivityStartCache() cache.Cache {
	return cache.New(1024, &cache.Options{
		TTL: activityStartReceiptTTL,
		OnEvict: func(value any) {
			value.(*withheldActivityStart).used.Store(true)
		},
	})
}

type withheldActivityStart struct {
	worker       *pollMetadata
	queue        PhysicalTaskQueueVersion
	target       *deploymentspb.WorkerDeploymentVersion
	redirect     *taskqueuespb.BuildIdRedirectInfo
	revision     int64
	expires      int64
	ownerStopped *atomic.Bool
	used         atomic.Bool
}

// All copies of a backlog row on its owning partition share the same receipt.
// The dispatch fields belong to this copy and are reset when it re-enters the matcher.
type activityRecoveryDispatch struct {
	start     *withheldActivityStart
	sameRoute bool
	claimed   bool
}

func (r *activityRecoveryDispatch) pending(now int64) bool {
	return r != nil && now < r.start.expires && !r.start.ownerStopped.Load() && !r.start.used.Load()
}

func (r *activityRecoveryDispatch) accepts(poller *waitingPoller) bool {
	p := poller.pollMetadata
	if !r.sameRoute || poller.taskForwarderType != notTaskForwarder || p == nil {
		return false
	}
	w := r.start.worker
	samePoll := w.pollID != "" && w.pollID == p.pollID
	sameInstance := w.workerInstanceKey != "" && w.workerInstanceKey == p.workerInstanceKey
	return (samePoll || sameInstance) && w.identity == p.identity && w.workerControlTaskQueue == p.workerControlTaskQueue &&
		w.workerVersionCapabilities.Equal(p.workerVersionCapabilities) && w.deploymentOptions.Equal(p.deploymentOptions)
}
