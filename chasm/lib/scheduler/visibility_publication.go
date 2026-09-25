package scheduler

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Scheduler) PrepareVisibility(ctx chasm.MutableContext) (bool, error) {
	if s.Sentinel {
		return false, nil
	}

	interval := tweakablesFromContext(ctx).VisibilityCoalesceInterval
	if interval <= 0 {
		s.visibilityForcePublish = false
		if s.VisibilityPublication == nil {
			return false, nil
		}
		live := s.currentVisibilityPublication(ctx)
		previous := s.VisibilityPublication
		if previous.GetRefreshDeadline() == nil && sameVisibilityPublication(previous, live) {
			return false, nil
		}
		live.RefreshGeneration = previous.GetRefreshGeneration() + 1
		s.VisibilityPublication = live
		metrics.ScheduleVisibilityPublicationImmediateCount.With(ctx.MetricsHandler()).Record(1)
		return true, nil
	}

	live := s.currentVisibilityPublication(ctx)
	previous := s.VisibilityPublication
	if previous == nil {
		s.VisibilityPublication = live
		s.visibilityForcePublish = false
		metrics.ScheduleVisibilityPublicationImmediateCount.With(ctx.MetricsHandler()).Record(1)
		return true, nil
	}

	if sameVisibilityPublication(previous, live) && !s.visibilityForcePublish {
		return false, nil
	}

	immediate := s.visibilityForcePublish || s.Closed ||
		previous.GetPaused() != live.GetPaused() ||
		previous.GetExecutionStatus() != live.GetExecutionStatus() ||
		!proto.Equal(previous.GetIdleCloseTime(), live.GetIdleCloseTime()) ||
		!proto.Equal(previous.GetListInfo().GetSpec(), live.GetListInfo().GetSpec()) ||
		!proto.Equal(previous.GetListInfo().GetWorkflowType(), live.GetListInfo().GetWorkflowType()) ||
		previous.GetListInfo().GetNotes() != live.GetListInfo().GetNotes()
	if immediate {
		live.RefreshGeneration = previous.GetRefreshGeneration() + 1
		s.VisibilityPublication = live
		s.visibilityForcePublish = false
		metrics.ScheduleVisibilityPublicationImmediateCount.With(ctx.MetricsHandler()).Record(1)
		return true, nil
	}

	if previous.GetRefreshDeadline() != nil {
		metrics.ScheduleVisibilityPublicationDeferredCount.With(ctx.MetricsHandler()).Record(1)
		return false, nil
	}

	deadline := ctx.Now(s).Add(interval)
	previous.RefreshDeadline = timestamppb.New(deadline)
	previous.RefreshGeneration++
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: deadline},
		&schedulerpb.SchedulerVisibilityRefreshTask{Generation: previous.RefreshGeneration})
	metrics.ScheduleVisibilityPublicationDeferredCount.With(ctx.MetricsHandler()).Record(1)
	return true, nil
}

func sameVisibilityPublication(a, b *schedulerpb.VisibilityPublication) bool {
	return proto.Equal(a.GetListInfo(), b.GetListInfo()) &&
		proto.Equal(a.GetNextActionTime(), b.GetNextActionTime()) &&
		proto.Equal(a.GetIdleCloseTime(), b.GetIdleCloseTime()) &&
		a.GetRunningWorkflowCount() == b.GetRunningWorkflowCount() &&
		a.GetBufferedStartsCount() == b.GetBufferedStartsCount() &&
		a.GetPaused() == b.GetPaused() &&
		a.GetExecutionStatus() == b.GetExecutionStatus()
}

func (s *Scheduler) currentVisibilityPublication(ctx chasm.Context) *schedulerpb.VisibilityPublication {
	publication := &schedulerpb.VisibilityPublication{
		ListInfo:        common.CloneProto(s.ListInfo(ctx)),
		Paused:          s.Schedule.GetState().GetPaused(),
		ExecutionStatus: s.executionStatus(),
	}
	if s.IdleCloseTime != nil {
		publication.IdleCloseTime = common.CloneProto(s.IdleCloseTime)
	}
	if !s.Closed {
		generator := s.Generator.Get(ctx)
		if len(generator.FutureActionTimes) > 0 {
			publication.NextActionTime = common.CloneProto(generator.FutureActionTimes[0])
		}
		invoker := s.Invoker.Get(ctx)
		publication.RunningWorkflowCount = int64(len(invoker.runningWorkflowExecutions()))
		publication.BufferedStartsCount = int64(invoker.bufferedStartsCount())
	}
	return publication
}

func (s *Scheduler) publishedSearchAttributes() []chasm.SearchAttributeKeyValue {
	published := s.VisibilityPublication
	out := []chasm.SearchAttributeKeyValue{
		executionStatusSearchAttribute.Value(published.ExecutionStatus),
		chasm.SearchAttributeTemporalSchedulePaused.Value(published.Paused),
	}
	if published.ExecutionStatus == executionStatusRunning {
		if published.NextActionTime != nil {
			out = append(out, scheduleNextActionTimeSearchAttribute.Value(published.NextActionTime.AsTime()))
		}
		if published.IdleCloseTime != nil {
			out = append(out, scheduleIdleCloseTimeSearchAttribute.Value(published.IdleCloseTime.AsTime()))
		}
		out = append(out,
			scheduleRunningWorkflowCountSearchAttribute.Value(published.RunningWorkflowCount),
			scheduleBufferedStartsCountSearchAttribute.Value(published.BufferedStartsCount),
		)
	}
	return out
}

type SchedulerVisibilityRefreshTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *SchedulerVisibilityRefreshTaskHandler) Validate(
	_ chasm.Context,
	s *Scheduler,
	invocation chasm.TaskInvocation,
	task *schedulerpb.SchedulerVisibilityRefreshTask,
) (bool, error) {
	published := s.VisibilityPublication
	return !s.Closed && published != nil &&
		published.GetRefreshDeadline() != nil &&
		published.GetRefreshGeneration() == task.GetGeneration() &&
		published.GetRefreshDeadline().AsTime().Equal(invocation.ScheduledTime), nil
}

func (h *SchedulerVisibilityRefreshTaskHandler) Execute(
	ctx chasm.MutableContext,
	s *Scheduler,
	_ chasm.TaskAttributes,
	_ *schedulerpb.SchedulerVisibilityRefreshTask,
) error {
	s.VisibilityPublication.RefreshDeadline = nil
	s.VisibilityPublication.RefreshGeneration++
	s.visibilityForcePublish = true
	metrics.ScheduleVisibilityPublicationRefreshCount.With(ctx.MetricsHandler()).Record(1)
	return nil
}
