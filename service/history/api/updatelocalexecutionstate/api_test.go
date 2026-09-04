package updatelocalexecutionstate

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestApplyTransition(t *testing.T) {
	request := &adminservice.UpdateLocalExecutionStateRequest{
		LocalServerId: "bridge-a",
		FencingEpoch:  7,
	}
	localInfo := &persistencespb.LocalExecutionInfo{}

	require.NoError(t, applyTransition(
		localInfo,
		request,
		persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE,
	))
	require.Equal(t, "bridge-a", localInfo.GetLocalServerId())
	require.Equal(t, int64(7), localInfo.GetFencingEpoch())

	require.NoError(t, applyTransition(
		localInfo,
		request,
		persistencespb.LocalExecutionInfo_BRIDGE_STATE_PAUSED,
	))
	require.NoError(t, applyTransition(
		localInfo,
		request,
		persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE,
	))
	require.NoError(t, applyTransition(
		localInfo,
		request,
		persistencespb.LocalExecutionInfo_BRIDGE_STATE_OWNERSHIP_LOST,
	))

	err := applyTransition(
		localInfo,
		request,
		persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE,
	)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
}

func TestApplyTransitionInitializesAtPauseAndRejectsStaleEpoch(t *testing.T) {
	request := &adminservice.UpdateLocalExecutionStateRequest{
		LocalServerId: "bridge-a",
		FencingEpoch:  7,
	}
	localInfo := &persistencespb.LocalExecutionInfo{}
	err := applyTransition(
		localInfo,
		request,
		persistencespb.LocalExecutionInfo_BRIDGE_STATE_PAUSED,
	)
	require.NoError(t, err)

	localInfo = &persistencespb.LocalExecutionInfo{
		LocalServerId: "bridge-a",
		FencingEpoch:  8,
		BridgeState:   persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE,
	}
	err = applyTransition(
		localInfo,
		request,
		persistencespb.LocalExecutionInfo_BRIDGE_STATE_PAUSED,
	)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
}
