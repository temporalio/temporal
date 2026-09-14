package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func newTestBranchToken(
	t *testing.T,
	treeID string,
	branchID string,
	ancestors []*persistencespb.HistoryBranchRange,
) []byte {
	t.Helper()
	branchUtil := persistence.NewHistoryBranchUtil(serialization.NewSerializer())
	token, err := branchUtil.NewHistoryBranch(
		"namespace-id", "workflow-id", "run-id", treeID, &branchID, ancestors, 0, 0, 0,
	)
	require.NoError(t, err)
	return token
}

func testVersionHistories(tokens ...[]byte) *historyspb.VersionHistories {
	versionHistories := &historyspb.VersionHistories{}
	for _, token := range tokens {
		versionHistories.Histories = append(
			versionHistories.Histories,
			&historyspb.VersionHistory{BranchToken: token},
		)
	}
	return versionHistories
}

func TestBranchTokenMismatchReason(t *testing.T) {
	treeID := primitives.NewUUID().String()
	branchID := primitives.NewUUID().String()
	otherTreeID := primitives.NewUUID().String()
	otherBranchID := primitives.NewUUID().String()

	current := newTestBranchToken(t, treeID, branchID, nil)
	nonCurrent := newTestBranchToken(t, otherTreeID, otherBranchID, nil)

	t.Run("matches the current branch token", func(t *testing.T) {
		got := branchTokenMismatchReason(current, current, testVersionHistories(current))
		require.Empty(t, got)
	})

	t.Run("matches the current token when an identical token is recorded twice", func(t *testing.T) {
		got := branchTokenMismatchReason(current, current, testVersionHistories(current, current))
		require.Empty(t, got)
	})

	t.Run("matches opaque tokens the branch parser cannot read", func(t *testing.T) {
		opaque := []byte{1, 2, 3}
		got := branchTokenMismatchReason(opaque, opaque, testVersionHistories(opaque))
		require.Empty(t, got)
	})

	t.Run("reports a non-current branch when the token names an older history", func(t *testing.T) {
		histories := testVersionHistories(nonCurrent, current)
		got := branchTokenMismatchReason(current, nonCurrent, histories)
		require.Equal(t, branchTokenMismatchReasonNonCurrent, got)
	})

	t.Run("reports foreign for a different branch in the same tree", func(t *testing.T) {
		request := newTestBranchToken(t, treeID, otherBranchID, nil)
		got := branchTokenMismatchReason(current, request, testVersionHistories(current))
		require.Equal(t, branchTokenMismatchReasonForeign, got)
	})

	t.Run("reports foreign for the current branch carrying injected ancestors", func(t *testing.T) {
		request := newTestBranchToken(t, treeID, branchID, []*persistencespb.HistoryBranchRange{
			{BranchId: otherBranchID, BeginNodeId: 1, EndNodeId: 1000},
		})
		got := branchTokenMismatchReason(current, request, testVersionHistories(current))
		require.Equal(t, branchTokenMismatchReasonForeign, got)
	})

	t.Run("reports foreign when there are no version histories", func(t *testing.T) {
		got := branchTokenMismatchReason(current, nonCurrent, nil)
		require.Equal(t, branchTokenMismatchReasonForeign, got)
	})
}

func TestValidateBranchTokenForExecution_EmptyRequestToken(t *testing.T) {
	for _, tc := range []struct {
		name       string
		validation bool
		wantErr    error
	}{
		{name: "rejected while validating", validation: true, wantErr: consts.ErrInvalidNextPageToken},
		{name: "served once validation is disabled", validation: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shardContext := historyi.NewMockShardContext(gomock.NewController(t))
			shardContext.EXPECT().GetConfig().Return(&configs.Config{
				EnablePaginationTokenBranchValidation: dynamicconfig.GetBoolPropertyFn(tc.validation),
			}).AnyTimes()

			err := ValidateBranchTokenForExecution(
				context.Background(), shardContext, nil, nil, "", "", nil, nil)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
