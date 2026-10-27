package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSingleSessionStickyPolicy(t *testing.T) {
	t.Parallel()
	for _, bound := range []bool{false, true} {
		name := "ordinary"
		if bound {
			name = "single session"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ms := &MutableStateImpl{executionInfo: &persistencespb.WorkflowExecutionInfo{
				TaskQueue: "foo", StickyTaskQueue: "S", SingleSessionBound: bound,
				StickyScheduleToStartTimeout: durationpb.New(time.Second),
			}}
			ms.SetStickyTaskQueue("other", durationpb.New(2*time.Second))
			if bound {
				require.Equal(t, "S", ms.executionInfo.GetStickyTaskQueue())
				ms.SetStickyTaskQueue("S", durationpb.New(3*time.Second))
				require.Equal(t, 3*time.Second, ms.executionInfo.GetStickyScheduleToStartTimeout().AsDuration())
				for _, queueName := range []string{"foo", "S"} {
					queue, timeout := ms.TaskQueueScheduleToStartTimeout(queueName)
					require.Equal(t, "S", queue.GetName())
					require.Equal(t, "foo", queue.GetNormalName())
					require.Equal(t, enumspb.TASK_QUEUE_KIND_STICKY, queue.GetKind())
					require.Equal(t, 5*time.Second, timeout.AsDuration())
				}
			} else {
				require.Equal(t, "other", ms.executionInfo.GetStickyTaskQueue())
			}
			ms.ClearStickyTaskQueue()
			if bound {
				require.Equal(t, "S", ms.executionInfo.GetStickyTaskQueue())
			} else {
				require.Empty(t, ms.executionInfo.GetStickyTaskQueue())
				require.Nil(t, ms.executionInfo.GetStickyScheduleToStartTimeout())
			}
			require.Equal(t, "foo", ms.executionInfo.GetTaskQueue())
		})
	}
}

func TestSingleSessionWorkflowTaskTimers(t *testing.T) {
	t.Parallel()
	for _, bound := range []bool{false, true} {
		name := "ordinary"
		if bound {
			name = "single session"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ms := historyi.NewMockMutableState(ctrl)
			key := definition.NewWorkflowKey("namespace", "workflow", "run")
			info := &persistencespb.WorkflowExecutionInfo{SingleSessionBound: bound, StickyScheduleToStartTimeout: durationpb.New(time.Second)}
			ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
			ms.EXPECT().IsStickyTaskQueueSet().Return(true).AnyTimes()
			ms.EXPECT().GetWorkflowKey().Return(key).AnyTimes()
			wt := &historyi.WorkflowTaskInfo{
				ScheduledEventID: 5, ScheduledTime: time.Now(), Attempt: 1,
				TaskQueue: &taskqueuepb.TaskQueue{Name: "S", Kind: enumspb.TASK_QUEUE_KIND_STICKY},
				Type:      enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
			}
			ms.EXPECT().GetWorkflowTaskByID(int64(5)).Return(wt)
			expectedTimeout := time.Second
			if bound {
				expectedTimeout = 5 * time.Second
			}
			ms.EXPECT().TaskQueueScheduleToStartTimeout("S").Return(wt.TaskQueue, durationpb.New(expectedTimeout)).Times(2)
			ms.EXPECT().AddTasks(gomock.AssignableToTypeOf(&tasks.WorkflowTaskTimeoutTask{})).Do(func(task tasks.Task) {
				timer := task.(*tasks.WorkflowTaskTimeoutTask)
				require.Equal(t, wt.ScheduledTime.Add(expectedTimeout), timer.VisibilityTimestamp)
				require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, timer.TimeoutType)
			})
			ms.EXPECT().SetWorkflowTaskScheduleToStartTimeoutTask(gomock.Any())
			ms.EXPECT().AddTasks(gomock.AssignableToTypeOf(&tasks.WorkflowTask{})).Do(func(task tasks.Task) {
				require.Equal(t, "S", task.(*tasks.WorkflowTask).TaskQueue)
			})
			generator := &TaskGeneratorImpl{mutableState: ms}
			require.NoError(t, generator.GenerateScheduleWorkflowTaskTasks(5))

			wt.Type = enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
			ms.EXPECT().SetSpeculativeWorkflowTaskTimeoutTask(gomock.Any()).DoAndReturn(func(task *tasks.WorkflowTaskTimeoutTask) error {
				require.Equal(t, wt.ScheduledTime.Add(expectedTimeout), task.VisibilityTimestamp)
				return nil
			})
			require.NoError(t, generator.GenerateScheduleSpeculativeWorkflowTaskTasks(wt))
		})
	}
}
