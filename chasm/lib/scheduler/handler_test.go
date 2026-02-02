package scheduler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSentinelHandlers(t *testing.T) {
	tests := []struct {
		name    string
		callFn  func(ctx context.Context, ctrl *gomock.Controller, mockEngine *chasm.MockEngine, sentinel *scheduler.Scheduler, chasmCtx chasm.Context) error
		isRead  bool // true for read operations, false for update operations
	}{
		{
			name: "DescribeSchedule",
			callFn: func(ctx context.Context, ctrl *gomock.Controller, mockEngine *chasm.MockEngine, sentinel *scheduler.Scheduler, chasmCtx chasm.Context) error {
				mockEngine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
						return readFn(chasmCtx, sentinel)
					}).Times(1)

				specBuilder := legacyscheduler.NewSpecBuilder()
				h := scheduler.NewTestHandler(nil, specBuilder)
				_, err := h.DescribeSchedule(ctx, &schedulerpb.DescribeScheduleRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.DescribeScheduleRequest{
						Namespace:  namespace,
						ScheduleId: scheduleID,
					},
				})
				return err
			},
			isRead: true,
		},
		{
			name: "ListScheduleMatchingTimes",
			callFn: func(ctx context.Context, ctrl *gomock.Controller, mockEngine *chasm.MockEngine, sentinel *scheduler.Scheduler, chasmCtx chasm.Context) error {
				mockEngine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
						return readFn(chasmCtx, sentinel)
					}).Times(1)

				specBuilder := legacyscheduler.NewSpecBuilder()
				h := scheduler.NewTestHandler(nil, specBuilder)
				_, err := h.ListScheduleMatchingTimes(ctx, &schedulerpb.ListScheduleMatchingTimesRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.ListScheduleMatchingTimesRequest{
						Namespace:  namespace,
						ScheduleId: scheduleID,
						StartTime:  timestamppb.Now(),
						EndTime:    timestamppb.Now(),
					},
				})
				return err
			},
			isRead: true,
		},
		{
			name: "UpdateSchedule",
			callFn: func(ctx context.Context, ctrl *gomock.Controller, mockEngine *chasm.MockEngine, sentinel *scheduler.Scheduler, chasmCtx chasm.Context) error {
				mockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
						err := updateFn(chasmCtx.(chasm.MutableContext), sentinel)
						return nil, err
					}).Times(1)

				h := scheduler.NewTestHandler(nil, nil)
				_, err := h.UpdateSchedule(ctx, &schedulerpb.UpdateScheduleRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.UpdateScheduleRequest{
						Namespace:  namespace,
						ScheduleId: scheduleID,
					},
				})
				return err
			},
			isRead: false,
		},
		{
			name: "PatchSchedule",
			callFn: func(ctx context.Context, ctrl *gomock.Controller, mockEngine *chasm.MockEngine, sentinel *scheduler.Scheduler, chasmCtx chasm.Context) error {
				mockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
						err := updateFn(chasmCtx.(chasm.MutableContext), sentinel)
						return nil, err
					}).Times(1)

				h := scheduler.NewTestHandler(nil, nil)
				_, err := h.PatchSchedule(ctx, &schedulerpb.PatchScheduleRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.PatchScheduleRequest{
						Namespace:  namespace,
						ScheduleId: scheduleID,
					},
				})
				return err
			},
			isRead: false,
		},
		{
			name: "DeleteSchedule",
			callFn: func(ctx context.Context, ctrl *gomock.Controller, mockEngine *chasm.MockEngine, sentinel *scheduler.Scheduler, chasmCtx chasm.Context) error {
				mockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
						err := updateFn(chasmCtx.(chasm.MutableContext), sentinel)
						return nil, err
					}).Times(1)

				h := scheduler.NewTestHandler(nil, nil)
				_, err := h.DeleteSchedule(ctx, &schedulerpb.DeleteScheduleRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.DeleteScheduleRequest{
						Namespace:  namespace,
						ScheduleId: scheduleID,
					},
				})
				return err
			},
			isRead: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sentinel, chasmCtx, _ := setupSentinelForTest(t)

			ctrl := gomock.NewController(t)
			mockEngine := chasm.NewMockEngine(ctrl)
			ctx := chasm.NewEngineContext(context.Background(), mockEngine)

			err := tc.callFn(ctx, ctrl, mockEngine, sentinel, chasmCtx)

			require.Error(t, err)
			var failedPreconditionErr *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPreconditionErr, "expected FailedPrecondition error for sentinel")
			require.Contains(t, err.Error(), "sentinel")
		})
	}
}
