package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestValidateLocalExecutionTask(t *testing.T) {
	for _, test := range []struct {
		name      string
		state     persistencespb.LocalExecutionInfo_BridgeState
		errorType string
	}{
		{name: "runnable", state: persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE},
		{name: "paused", state: persistencespb.LocalExecutionInfo_BRIDGE_STATE_PAUSED, errorType: "unavailable"},
		{name: "ownership lost", state: persistencespb.LocalExecutionInfo_BRIDGE_STATE_OWNERSHIP_LOST, errorType: "not found"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutableState := historyi.NewMockMutableState(gomock.NewController(t))
			mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				LocalExecutionInfo: &persistencespb.LocalExecutionInfo{BridgeState: test.state},
			})

			err := ValidateLocalExecutionTask(mutableState)
			switch test.errorType {
			case "":
				require.NoError(t, err)
			case "unavailable":
				require.ErrorAs(t, err, new(*serviceerror.Unavailable))
			default:
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			}
		})
	}
}
