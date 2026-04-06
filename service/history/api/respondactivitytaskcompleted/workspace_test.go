package respondactivitytaskcompleted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	workspacepb "go.temporal.io/api/workspace/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestApplyWorkspaceCommit(t *testing.T) {
	t.Run("workspace not found - nil map", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ms := historyi.NewMockMutableState(ctrl)
		ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{})

		err := applyWorkspaceCommit(ms, &workspacepb.WorkspaceCommit{
			WorkspaceId: "ws-1",
			NewVersion:  1,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("workspace not found - missing ID", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ms := historyi.NewMockMutableState(ctrl)
		ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			WorkspaceInfos: map[string]*workspacepb.WorkspaceInfo{
				"ws-other": {WorkspaceId: "ws-other"},
			},
		})

		err := applyWorkspaceCommit(ms, &workspacepb.WorkspaceCommit{
			WorkspaceId: "ws-1",
			NewVersion:  1,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("version mismatch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ms := historyi.NewMockMutableState(ctrl)
		ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			WorkspaceInfos: map[string]*workspacepb.WorkspaceInfo{
				"ws-1": {WorkspaceId: "ws-1", CommittedVersion: 3},
			},
		})

		err := applyWorkspaceCommit(ms, &workspacepb.WorkspaceCommit{
			WorkspaceId: "ws-1",
			NewVersion:  5, // expected 4
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "version mismatch")
	})

	t.Run("diff too large", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ms := historyi.NewMockMutableState(ctrl)
		ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			WorkspaceInfos: map[string]*workspacepb.WorkspaceInfo{
				"ws-1": {WorkspaceId: "ws-1", CommittedVersion: 0},
			},
		})

		err := applyWorkspaceCommit(ms, &workspacepb.WorkspaceCommit{
			WorkspaceId:   "ws-1",
			NewVersion:    1,
			DiffSizeBytes: defaultMaxDiffSizeBytes + 1,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds limit")
	})

	t.Run("successful commit with diff", func(t *testing.T) {
		wsInfo := &workspacepb.WorkspaceInfo{
			WorkspaceId:      "ws-1",
			CommittedVersion: 2,
		}
		ctrl := gomock.NewController(t)
		ms := historyi.NewMockMutableState(ctrl)
		ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			WorkspaceInfos: map[string]*workspacepb.WorkspaceInfo{
				"ws-1": wsInfo,
			},
		})

		err := applyWorkspaceCommit(ms, &workspacepb.WorkspaceCommit{
			WorkspaceId:   "ws-1",
			NewVersion:    3,
			DriverName:    "s3",
			DiffClaim:     map[string]string{"bucket": "my-bucket", "key": "diffs/ws-1/v2_v3"},
			DiffSizeBytes: 4096,
		})
		require.NoError(t, err)

		assert.Equal(t, int64(3), wsInfo.CommittedVersion)
		require.Len(t, wsInfo.Diffs, 1)
		assert.Equal(t, int64(2), wsInfo.Diffs[0].FromVersion)
		assert.Equal(t, int64(3), wsInfo.Diffs[0].ToVersion)
		assert.Equal(t, int64(4096), wsInfo.Diffs[0].SizeBytes)
		assert.Equal(t, "s3", wsInfo.Diffs[0].DriverName)
		assert.Equal(t, "my-bucket", wsInfo.Diffs[0].Claim["bucket"])
	})

	t.Run("successful commit without diff (no filesystem changes)", func(t *testing.T) {
		wsInfo := &workspacepb.WorkspaceInfo{
			WorkspaceId:      "ws-1",
			CommittedVersion: 0,
		}
		ctrl := gomock.NewController(t)
		ms := historyi.NewMockMutableState(ctrl)
		ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			WorkspaceInfos: map[string]*workspacepb.WorkspaceInfo{
				"ws-1": wsInfo,
			},
		})

		err := applyWorkspaceCommit(ms, &workspacepb.WorkspaceCommit{
			WorkspaceId:   "ws-1",
			NewVersion:    1,
			DiffSizeBytes: 0,
		})
		require.NoError(t, err)

		assert.Equal(t, int64(1), wsInfo.CommittedVersion)
		assert.Empty(t, wsInfo.Diffs)
	})
}
