package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
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

func TestPauseLocalExecutionForRemoteCommands(t *testing.T) {
	activityCommand := func(activityType string, taskQueue string) *commandpb.Command {
		return &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityType: &commonpb.ActivityType{Name: activityType},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
				},
			},
		}
	}
	for _, test := range []struct {
		name      string
		command   *commandpb.Command
		taskQueue string
		paused    bool
	}{
		{name: "registered Activity", command: activityCommand("local", "queue"), taskQueue: "queue"},
		{name: "unregistered Activity", command: activityCommand("remote", "queue"), taskQueue: "queue", paused: true},
		{name: "differently routed Activity", command: activityCommand("local", "remote"), taskQueue: "queue", paused: true},
		{name: "Nexus operation", command: &commandpb.Command{CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION}, taskQueue: "queue", paused: true},
		{name: "external signal", command: &commandpb.Command{CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION}, taskQueue: "queue", paused: true},
		{name: "external cancellation", command: &commandpb.Command{CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION}, taskQueue: "queue", paused: true},
		{name: "boundary detection disabled", command: activityCommand("remote", "queue")},
	} {
		t.Run(test.name, func(t *testing.T) {
			info := &persistencespb.WorkflowExecutionInfo{
				LocalExecutionInfo: &persistencespb.LocalExecutionInfo{
					BridgeState: persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE,
				},
			}
			PauseLocalExecutionForRemoteCommands(info, []*commandpb.Command{test.command}, test.taskQueue, []string{"local"})
			expected := persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE
			if test.paused {
				expected = persistencespb.LocalExecutionInfo_BRIDGE_STATE_PAUSED
			}
			require.Equal(t, expected, info.GetLocalExecutionInfo().GetBridgeState())
		})
	}
}

func TestDisableLocalExecutionEagerActivities(t *testing.T) {
	registered := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityType:          &commonpb.ActivityType{Name: "local"},
				TaskQueue:             &taskqueuepb.TaskQueue{Name: "queue"},
				RequestEagerExecution: true,
			},
		},
	}
	remote := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityType:          &commonpb.ActivityType{Name: "remote"},
				TaskQueue:             &taskqueuepb.TaskQueue{Name: "queue"},
				RequestEagerExecution: true,
			},
		},
	}

	DisableLocalExecutionEagerActivities([]*commandpb.Command{registered, remote}, "queue", []string{"local"})
	require.True(t, registered.GetScheduleActivityTaskCommandAttributes().GetRequestEagerExecution())
	require.False(t, remote.GetScheduleActivityTaskCommandAttributes().GetRequestEagerExecution())
}
