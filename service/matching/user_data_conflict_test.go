package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
)

func TestUserDataUpdateConflict(t *testing.T) {
	t.Parallel()

	for _, version := range []int64{0, 2} {
		name := "existing user data"
		if version == 0 {
			name = "new user data"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			controller := gomock.NewController(t)
			opts := defaultTqmTestOpts(controller)
			m := createUserDataManager(t, controller, opts)
			store := persistence.NewMockTaskManager(controller)
			m.store = store
			cached := &persistencespb.VersionedTaskQueueUserData{Version: version}
			persisted := &persistencespb.VersionedTaskQueueUserData{Version: version + 1, Data: mkUserData(1)}
			getRequest := &persistence.GetTaskQueueUserDataRequest{
				NamespaceID: m.partition.NamespaceId(),
				TaskQueue:   m.partition.TaskQueue().Name(),
			}
			expectedData := common.CloneProto(persisted.Data)
			expectedData.Clock = &clockspb.HybridLogicalClock{WallClock: 42}
			gomock.InOrder(
				store.EXPECT().GetTaskQueueUserData(gomock.Any(), getRequest).
					Return(&persistence.GetTaskQueueUserDataResponse{UserData: cached}, nil),
				opts.matchingClientMock.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any()).
					Return(nil, serviceerror.NewInvalidArgument("user data version conflict")),
				store.EXPECT().GetTaskQueueUserData(gomock.Any(), getRequest).
					Return(&persistence.GetTaskQueueUserDataResponse{UserData: persisted}, nil),
				opts.matchingClientMock.EXPECT().UpdateTaskQueueUserData(gomock.Any(),
					&matchingservice.UpdateTaskQueueUserDataRequest{
						NamespaceId: m.partition.NamespaceId(),
						TaskQueue:   m.partition.TaskQueue().Name(),
						UserData:    &persistencespb.VersionedTaskQueueUserData{Version: persisted.Version, Data: expectedData},
					}).Return(&matchingservice.UpdateTaskQueueUserDataResponse{}, nil),
			)
			require.NoError(t, m.loadUserDataFromDB(t.Context()))
			m.setUserDataState(userDataEnabled, nil)
			update := func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
				updated := common.CloneProto(data)
				updated.Clock = &clockspb.HybridLogicalClock{WallClock: 42}
				return updated, false, nil
			}

			_, err := m.UpdateUserData(t.Context(), UserDataUpdateOptions{}, update)
			require.ErrorAs(t, err, new(*serviceerror.Unavailable))
			got, _, err := m.GetUserData()
			require.NoError(t, err)
			protorequire.ProtoEqual(t, persisted, got)

			// Retry the whole update so the callback preserves the old owner's committed data.
			gotVersion, err := m.UpdateUserData(t.Context(), UserDataUpdateOptions{}, update)
			require.NoError(t, err)
			require.Equal(t, persisted.Version+1, gotVersion)
			got, _, err = m.GetUserData()
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &persistencespb.VersionedTaskQueueUserData{
				Version: persisted.Version + 1,
				Data:    expectedData,
			}, got)
		})
	}
}

func TestUserDataUpdateRejected(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		writeErr    error
		refresh     bool
		refreshErr  error
		expectRetry bool
	}{
		{
			name:     "validation error without version change",
			writeErr: serviceerror.NewInvalidArgument("invalid update"),
			refresh:  true,
		},
		{
			name:        "refresh failure remains retryable",
			writeErr:    serviceerror.NewInvalidArgument("user data version conflict"),
			refresh:     true,
			refreshErr:  serviceerror.NewUnavailable("persistence unavailable"),
			expectRetry: true,
		},
		{
			name:     "other write error",
			writeErr: serviceerror.NewUnavailable("write unavailable"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			controller := gomock.NewController(t)
			opts := defaultTqmTestOpts(controller)
			m := createUserDataManager(t, controller, opts)
			store := persistence.NewMockTaskManager(controller)
			m.store = store
			cached := &persistencespb.VersionedTaskQueueUserData{Version: 1, Data: mkUserData(1)}
			getRequest := &persistence.GetTaskQueueUserDataRequest{
				NamespaceID: m.partition.NamespaceId(),
				TaskQueue:   m.partition.TaskQueue().Name(),
			}
			load := store.EXPECT().GetTaskQueueUserData(gomock.Any(), getRequest).
				Return(&persistence.GetTaskQueueUserDataResponse{UserData: cached}, nil)
			write := opts.matchingClientMock.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any()).
				Return(nil, tc.writeErr).After(load)
			if tc.refresh {
				store.EXPECT().GetTaskQueueUserData(gomock.Any(), getRequest).
					Return(&persistence.GetTaskQueueUserDataResponse{UserData: cached}, tc.refreshErr).After(write)
			}
			require.NoError(t, m.loadUserDataFromDB(t.Context()))
			m.setUserDataState(userDataEnabled, nil)

			_, err := m.UpdateUserData(t.Context(), UserDataUpdateOptions{},
				func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
					return common.CloneProto(data), false, nil
				})
			if tc.expectRetry {
				require.ErrorAs(t, err, new(*serviceerror.Unavailable))
			} else {
				require.ErrorIs(t, err, tc.writeErr)
			}
			got, _, err := m.GetUserData()
			require.NoError(t, err)
			protorequire.ProtoEqual(t, cached, got)
		})
	}
}
