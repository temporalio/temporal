package persistence

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
)

type (
	historyBranchUtilSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestHistoryBranchUtilSuite(t *testing.T) {
	s := new(historyBranchUtilSuite)
	suite.Run(t, s)
}

func (s *historyBranchUtilSuite) SetupSuite() {
}

func (s *historyBranchUtilSuite) TearDownSuite() {
}

func (s *historyBranchUtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyBranchUtilSuite) TearDownTest() {
}

func (s *historyBranchUtilSuite) TestHistoryBranchUtil() {
	var historyBranchUtil HistoryBranchUtil = NewHistoryBranchUtil(serialization.NewSerializer())

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
	s.NoError(err)

	branchInfo0, err := historyBranchUtil.ParseHistoryBranchInfo(branchToken0)
	s.NoError(err)
	s.Equal(treeID0, branchInfo0.GetTreeId())
	s.Equal(branchID0, branchInfo0.GetBranchId())
	s.Equal(ancestors, branchInfo0.GetAncestors())

	treeID1 := primitives.NewUUID().String()
	branchID1 := primitives.NewUUID().String()
	branchToken1, err := historyBranchUtil.UpdateHistoryBranchInfo(
		branchToken0,
		persistencespb.HistoryBranch_builder{
			TreeId:    treeID1,
			BranchId:  branchID1,
			Ancestors: ancestors,
		}.Build(),
		primitives.NewUUID().String(),
	)
	s.NoError(err)

	branchInfo1, err := historyBranchUtil.ParseHistoryBranchInfo(branchToken1)
	s.NoError(err)
	s.Equal(treeID1, branchInfo1.GetTreeId())
	s.Equal(branchID1, branchInfo1.GetBranchId())
	s.Equal(ancestors, branchInfo1.GetAncestors())
}
