package persistence

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/primitives"
)

type (
	historyBranchUtilSuite struct {
		suite.Suite
	}
)

func TestHistoryBranchUtilSuite(t *testing.T) {
	s := new(historyBranchUtilSuite)
	suite.Run(t, s)
}

func (s *historyBranchUtilSuite) TearDownSuite() {
}

func (s *historyBranchUtilSuite) TearDownTest() {
}

func (s *historyBranchUtilSuite) TestHistoryBranchUtil() {
	var historyBranchUtil HistoryBranchUtil = &HistoryBranchUtilImpl{}

	treeID0 := primitives.NewUUID().String()
	branchID0 := primitives.NewUUID().String()
	ancestors := []*persistencespb.HistoryBranchRange(nil)
	branchToken0, err := historyBranchUtil.NewHistoryBranch(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		treeID0,
		&branchID0,
		ancestors,
		0,
		0,
		0,
	)
	require.NoError(s.T(), err)

	branchInfo0, err := historyBranchUtil.ParseHistoryBranchInfo(branchToken0)
	require.NoError(s.T(), err)
	require.Equal(s.T(), treeID0, branchInfo0.TreeId)
	require.Equal(s.T(), branchID0, branchInfo0.BranchId)
	require.Equal(s.T(), ancestors, branchInfo0.Ancestors)

	treeID1 := primitives.NewUUID().String()
	branchID1 := primitives.NewUUID().String()
	branchToken1, err := historyBranchUtil.UpdateHistoryBranchInfo(
		branchToken0,
		&persistencespb.HistoryBranch{
			TreeId:    treeID1,
			BranchId:  branchID1,
			Ancestors: ancestors,
		},
		primitives.NewUUID().String(),
	)
	require.NoError(s.T(), err)

	branchInfo1, err := historyBranchUtil.ParseHistoryBranchInfo(branchToken1)
	require.NoError(s.T(), err)
	require.Equal(s.T(), treeID1, branchInfo1.TreeId)
	require.Equal(s.T(), branchID1, branchInfo1.BranchId)
	require.Equal(s.T(), ancestors, branchInfo1.Ancestors)
}
