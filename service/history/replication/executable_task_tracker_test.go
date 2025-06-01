package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
)

type (
	executableTaskTrackerSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		logger     log.Logger

		taskTracker *ExecutableTaskTrackerImpl
	}
)

func TestExecutableTaskTrackerSuite(t *testing.T) {
	s := new(executableTaskTrackerSuite)
	suite.Run(t, s)
}

func (s *executableTaskTrackerSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableTaskTrackerSuite) TearDownSuite() {

}

func (s *executableTaskTrackerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())

	s.taskTracker = NewExecutableTaskTracker(log.NewTestLogger(), metrics.NoopMetricsHandler)
}

func (s *executableTaskTrackerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableTaskTrackerSuite) TestTrackTasks() {
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	highWatermark0 := WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}

	tasks := s.taskTracker.TrackTasks(highWatermark0, task0)
	s.Equal([]TrackableExecutableTask{task0}, tasks)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID()}, taskIDs)
	s.Equal(highWatermark0, *s.taskTracker.exclusiveHighWatermarkInfo)

	task1 := NewMockTrackableExecutableTask(s.controller)
	task1.EXPECT().TaskID().Return(task0.TaskID() + 1).AnyTimes()
	task2 := NewMockTrackableExecutableTask(s.controller)
	task2.EXPECT().TaskID().Return(task1.TaskID() + 1).AnyTimes()
	highWatermark2 := WatermarkInfo{
		Watermark: task2.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}

	tasks = s.taskTracker.TrackTasks(highWatermark2, task1, task2)
	s.Equal([]TrackableExecutableTask{task1, task2}, tasks)

	taskIDs = []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID(), task1.TaskID(), task2.TaskID()}, taskIDs)
	s.Equal(highWatermark2, *s.taskTracker.exclusiveHighWatermarkInfo)
}

func (s *executableTaskTrackerSuite) TestTrackTasks_Duplication() {
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	highWatermark0 := WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := s.taskTracker.TrackTasks(highWatermark0, task0)
	s.Equal([]TrackableExecutableTask{task0}, tasks)
	tasks = s.taskTracker.TrackTasks(highWatermark0, task0)
	s.Equal([]TrackableExecutableTask{}, tasks)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID()}, taskIDs)
	s.Equal(highWatermark0, *s.taskTracker.exclusiveHighWatermarkInfo)

	task1 := NewMockTrackableExecutableTask(s.controller)
	task1.EXPECT().TaskID().Return(task0.TaskID() + 1).AnyTimes()
	highWatermark1 := WatermarkInfo{
		Watermark: task1.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks = s.taskTracker.TrackTasks(highWatermark1, task1)
	s.Equal([]TrackableExecutableTask{task1}, tasks)

	taskIDs = []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID(), task1.TaskID()}, taskIDs)
	s.Equal(highWatermark1, *s.taskTracker.exclusiveHighWatermarkInfo)

	task2 := NewMockTrackableExecutableTask(s.controller)
	task2.EXPECT().TaskID().Return(task1.TaskID() + 1).AnyTimes()
	highWatermark2 := WatermarkInfo{
		Watermark: task2.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks = s.taskTracker.TrackTasks(highWatermark2, task1, task2)
	s.Equal([]TrackableExecutableTask{task2}, tasks)

	taskIDs = []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID(), task1.TaskID(), task2.TaskID()}, taskIDs)
	s.Equal(highWatermark2, *s.taskTracker.exclusiveHighWatermarkInfo)
}

func (s *executableTaskTrackerSuite) TestTrackTasks_Cancellation() {
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().Cancel()
	highWatermark0 := WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}

	s.taskTracker.Cancel()
	tasks := s.taskTracker.TrackTasks(highWatermark0, task0)
	s.Equal([]TrackableExecutableTask{task0}, tasks)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID()}, taskIDs)
	s.Equal(highWatermark0, *s.taskTracker.exclusiveHighWatermarkInfo)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_Empty() {
	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{}, taskIDs)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Nil(lowWatermark)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_AckedTask_Case0() {
	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateAcked).AnyTimes()
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(s.controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	highWatermark0 := WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := s.taskTracker.TrackTasks(highWatermark0, task0, task1)
	s.Equal([]TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Equal(WatermarkInfo{
		Watermark: task1ID,
		Timestamp: task1.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task1ID}, taskIDs)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_AckedTask_Case1() {
	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateAcked).AnyTimes()
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(s.controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStateAcked).AnyTimes()
	highWatermark0 := WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := s.taskTracker.TrackTasks(highWatermark0, task0, task1)
	s.Equal([]TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Equal(highWatermark0, *lowWatermark)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{}, taskIDs)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_NackedTask_Success_Case0() {
	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task0.EXPECT().MarkPoisonPill().Return(nil)
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(s.controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()

	highWatermark0 := WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := s.taskTracker.TrackTasks(highWatermark0, task0, task1)
	s.Equal([]TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Equal(WatermarkInfo{
		Watermark: task1ID,
		Timestamp: task1.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task1ID}, taskIDs)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_NackedTask_Success_Case1() {
	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task0.EXPECT().MarkPoisonPill().Return(nil)
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(s.controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task1.EXPECT().MarkPoisonPill().Return(nil)

	highWatermark0 := WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := s.taskTracker.TrackTasks(highWatermark0, task0, task1)
	s.Equal([]TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Equal(highWatermark0, *lowWatermark)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{}, taskIDs)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_NackedTask_Error_Case0() {
	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task0.EXPECT().MarkPoisonPill().Return(errors.New("random error"))
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(s.controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()

	tasks := s.taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0, task1)
	s.Equal([]TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Equal(WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0ID, task1ID}, taskIDs)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_NackedTask_Error_Case1() {
	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task0.EXPECT().MarkPoisonPill().Return(serviceerror.NewInternal("random error"))
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(s.controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task1.EXPECT().MarkPoisonPill().Return(serviceerror.NewInternal("random error"))

	tasks := s.taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0, task1)
	s.Equal([]TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Equal(WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0ID, task1ID}, taskIDs)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_AbortedTask() {
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateAborted).AnyTimes()

	tasks := s.taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0)
	s.Equal([]TrackableExecutableTask{task0}, tasks)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Equal(WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID()}, taskIDs)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_CancelledTask() {
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateCancelled).AnyTimes()

	tasks := s.taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0)
	s.Equal([]TrackableExecutableTask{task0}, tasks)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Equal(WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID()}, taskIDs)
}

func (s *executableTaskTrackerSuite) TestLowWatermark_PendingTask() {
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()

	tasks := s.taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0)
	s.Equal([]TrackableExecutableTask{task0}, tasks)

	lowWatermark := s.taskTracker.LowWatermark()
	s.Equal(WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID()}, taskIDs)
}

func (s *executableTaskTrackerSuite) TestCancellation() {
	task0 := NewMockTrackableExecutableTask(s.controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().Cancel()
	highWatermark0 := WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}

	tasks := s.taskTracker.TrackTasks(highWatermark0, task0)
	s.Equal([]TrackableExecutableTask{task0}, tasks)
	s.taskTracker.Cancel()

	taskIDs := []int64{}
	for element := s.taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	s.Equal([]int64{task0.TaskID()}, taskIDs)
	s.Equal(highWatermark0, *s.taskTracker.exclusiveHighWatermarkInfo)
}
