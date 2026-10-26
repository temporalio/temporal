package persistence_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskQueueMetadataRetention(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		info     *persistencespb.TaskQueueInfo
		retained bool
	}{
		{name: "nil"},
		{name: "unassigned workflow sticky", info: &persistencespb.TaskQueueInfo{Kind: enumspb.TASK_QUEUE_KIND_STICKY, TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}},
		{name: "assigned workflow sticky", info: &persistencespb.TaskQueueInfo{Kind: enumspb.TASK_QUEUE_KIND_STICKY, TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, SingleSessionAssigned: true}, retained: true},
		{name: "activity sticky", info: &persistencespb.TaskQueueInfo{Kind: enumspb.TASK_QUEUE_KIND_STICKY, TaskType: enumspb.TASK_QUEUE_TYPE_ACTIVITY, SingleSessionAssigned: true}},
		{name: "normal workflow", info: &persistencespb.TaskQueueInfo{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, SingleSessionAssigned: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.retained, persistence.IsTaskQueueMetadataRetained(tc.info))
		})
	}
}

func TestTaskManagerSingleSessionMetadata(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"create", "update"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name       string
				assigned   bool
				taskType   enumspb.TaskQueueType
				expiry     bool
				invalid    bool
				storeError bool
			}{
				{name: "assigned workflow retained", assigned: true, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
				{name: "ordinary sticky expires", taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, expiry: true},
				{name: "unassigned requires expiry", taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, invalid: true},
				{name: "activity requires expiry", assigned: true, taskType: enumspb.TASK_QUEUE_TYPE_ACTIVITY, invalid: true},
				{name: "store failure", assigned: true, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, storeError: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					tv := testvars.New(t)
					store := mock.NewMockTaskStore(gomock.NewController(t))
					serializer := serialization.NewSerializer()
					manager := persistence.NewTaskManager(store, serializer)
					info := &persistencespb.TaskQueueInfo{
						NamespaceId:           tv.NamespaceID().String(),
						Name:                  tv.StickyTaskQueue().GetName(),
						TaskType:              tc.taskType,
						Kind:                  enumspb.TASK_QUEUE_KIND_STICKY,
						LastUpdateTime:        timestamppb.New(time.Unix(42, 0)),
						SingleSessionAssigned: tc.assigned,
					}
					if tc.expiry {
						info.ExpiryTime = timestamppb.New(time.Unix(3600, 0))
					}
					var storeErr error
					if tc.storeError {
						storeErr = serviceerror.NewUnavailable("store unavailable")
					}
					if !tc.invalid {
						if operation == "create" {
							store.EXPECT().CreateTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
								func(_ context.Context, req *persistence.InternalCreateTaskQueueRequest) error {
									restored, err := serializer.TaskQueueInfoFromBlob(req.TaskQueueInfo)
									require.NoError(t, err)
									protorequire.ProtoEqual(t, info, restored)
									protorequire.ProtoEqual(t, info.ExpiryTime, req.ExpiryTime)
									return storeErr
								})
						} else {
							store.EXPECT().UpdateTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
								func(_ context.Context, req *persistence.InternalUpdateTaskQueueRequest) (*persistence.UpdateTaskQueueResponse, error) {
									restored, err := serializer.TaskQueueInfoFromBlob(req.TaskQueueInfo)
									require.NoError(t, err)
									protorequire.ProtoEqual(t, info, restored)
									protorequire.ProtoEqual(t, info.ExpiryTime, req.ExpiryTime)
									require.Equal(t, persistence.IsTaskQueueMetadataRetained(info), req.SingleSessionAssigned)
									return &persistence.UpdateTaskQueueResponse{}, storeErr
								})
						}
					}
					invoke := func() {
						var err error
						if operation == "create" {
							_, err = manager.CreateTaskQueue(t.Context(), &persistence.CreateTaskQueueRequest{RangeID: 2, TaskQueueInfo: info})
						} else {
							_, err = manager.UpdateTaskQueue(t.Context(), &persistence.UpdateTaskQueueRequest{RangeID: 2, PrevRangeID: 1, TaskQueueInfo: info})
						}
						if tc.storeError {
							require.ErrorIs(t, err, storeErr)
						} else {
							require.NoError(t, err)
						}
					}
					if tc.invalid {
						require.Panics(t, invoke)
					} else {
						invoke()
					}
				})
			}
		})
	}
}
