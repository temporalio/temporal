// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	var historyBranchUtil HistoryBranchUtil = &HistoryBranchUtilImpl{}

	treeID0 := primitives.NewUUID().String()
	branchID0 := primitives.NewUUID().String()
	ancestors := []*persistencespb.HistoryBranchRange(nil)
	branchToken0, err := NewHistoryBranch(treeID0, &branchID0, ancestors)
	s.NoError(err)

	branchInfo0, err := historyBranchUtil.ParseHistoryBranchInfo(branchToken0)
	s.NoError(err)
	s.Equal(treeID0, branchInfo0.TreeId)
	s.Equal(branchID0, branchInfo0.BranchId)
	s.Equal(ancestors, branchInfo0.Ancestors)

	treeID1 := primitives.NewUUID().String()
	branchID1 := primitives.NewUUID().String()
	branchToken1, err := historyBranchUtil.UpdateHistoryBranchInfo(
		branchToken0,
		&persistencespb.HistoryBranch{
			TreeId:    treeID1,
			BranchId:  branchID1,
			Ancestors: ancestors,
		})
	s.NoError(err)

	branchInfo1, err := historyBranchUtil.ParseHistoryBranchInfo(branchToken1)
	s.NoError(err)
	s.Equal(treeID1, branchInfo1.TreeId)
	s.Equal(branchID1, branchInfo1.BranchId)
	s.Equal(ancestors, branchInfo1.Ancestors)
}
