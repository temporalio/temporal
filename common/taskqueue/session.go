package taskqueue

import (
	sessionpb "go.temporal.io/api/session/v1"
)

// SessionWorkerStickyQueue is the name of the sticky queue of a worker servicing sessions: a multi-session worker's
// sticky queue is named after its instance key, and a single-session worker's after its stable identity. It is "" for
// a worker without session options.
func SessionWorkerStickyQueue(options *sessionpb.WorkerSessionOptions, workerInstanceKey string) string {
	switch options.GetAdmission().(type) {
	case *sessionpb.WorkerSessionOptions_MultiSession:
		return workerInstanceKey
	case *sessionpb.WorkerSessionOptions_SingleSession:
		return options.GetSingleSession().GetStableIdentity()
	default:
		return ""
	}
}
