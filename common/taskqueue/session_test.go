package taskqueue

import (
	"testing"

	"github.com/stretchr/testify/require"
	sessionpb "go.temporal.io/api/session/v1"
)

func TestSessionWorkerStickyQueue(t *testing.T) {
	singleSession := &sessionpb.WorkerSessionOptions{Admission: &sessionpb.WorkerSessionOptions_SingleSession{
		SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: "stable-identity"},
	}}

	require.Equal(t, "stable-identity", SessionWorkerStickyQueue(singleSession, "instance-key"))
	require.Empty(t, SessionWorkerStickyQueue(nil, "instance-key"))
	require.Empty(t, SessionWorkerStickyQueue(&sessionpb.WorkerSessionOptions{}, "instance-key"))
}
