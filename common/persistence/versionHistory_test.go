// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"

	"github.com/stretchr/testify/suite"
)

type (
	versionHistorySuite struct {
		suite.Suite
	}

	versionHistoriesSuite struct {
		suite.Suite
	}
)

func TestVersionHistorySuite(t *testing.T) {
	s := new(versionHistorySuite)
	suite.Run(t, s)
}

func TestVersionHistoriesSuite(t *testing.T) {
	s := new(versionHistoriesSuite)
	suite.Run(t, s)
}

func (s *versionHistorySuite) TestConversion() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(branchToken, items)
	s.Equal(&VersionHistory{
		branchToken: branchToken,
		items:       items,
	}, history)

	s.Equal(history, NewVersionHistoryFromThrift(history.ToThrift()))
}

func (s *versionHistorySuite) TestDuplicateUntilLCAItem_Success() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(branchToken, items)

	newHistory, err := history.DuplicateUntilLCAItem(NewVersionHistoryItem(2, 0))
	s.NoError(err)
	newBranchToken := []byte("other random branch token")
	err = newHistory.SetBranchToken(newBranchToken)
	s.NoError(err)
	s.Equal(newBranchToken, newHistory.GetBranchToken())
	s.Equal(NewVersionHistory(
		newBranchToken,
		[]*VersionHistoryItem{{eventID: 2, version: 0}},
	), newHistory)

	newHistory, err = history.DuplicateUntilLCAItem(NewVersionHistoryItem(5, 4))
	s.NoError(err)
	newBranchToken = []byte("another random branch token")
	err = newHistory.SetBranchToken(newBranchToken)
	s.NoError(err)
	s.Equal(newBranchToken, newHistory.GetBranchToken())
	s.Equal(NewVersionHistory(
		newBranchToken,
		[]*VersionHistoryItem{
			{eventID: 3, version: 0},
			{eventID: 5, version: 4},
		},
	), newHistory)

	newHistory, err = history.DuplicateUntilLCAItem(NewVersionHistoryItem(6, 4))
	s.NoError(err)
	newBranchToken = []byte("yet another random branch token")
	err = newHistory.SetBranchToken(newBranchToken)
	s.NoError(err)
	s.Equal(newBranchToken, newHistory.GetBranchToken())
	s.Equal(NewVersionHistory(
		newBranchToken,
		[]*VersionHistoryItem{
			{eventID: 3, version: 0},
			{eventID: 6, version: 4},
		},
	), newHistory)
}

func (s *versionHistorySuite) TestDuplicateUntilLCAItem_Failure() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(branchToken, items)

	_, err := history.DuplicateUntilLCAItem(NewVersionHistoryItem(4, 0))
	s.IsType(&shared.BadRequestError{}, err)

	_, err = history.DuplicateUntilLCAItem(NewVersionHistoryItem(2, 1))
	s.IsType(&shared.BadRequestError{}, err)

	_, err = history.DuplicateUntilLCAItem(NewVersionHistoryItem(5, 3))
	s.IsType(&shared.BadRequestError{}, err)

	_, err = history.DuplicateUntilLCAItem(NewVersionHistoryItem(7, 5))
	s.IsType(&shared.BadRequestError{}, err)

	_, err = history.DuplicateUntilLCAItem(NewVersionHistoryItem(4, 0))
	s.IsType(&shared.BadRequestError{}, err)

	_, err = history.DuplicateUntilLCAItem(NewVersionHistoryItem(7, 4))
	s.IsType(&shared.BadRequestError{}, err)
}

func (s *versionHistorySuite) TestSetBranchToken() {
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(nil, items)

	err := history.SetBranchToken([]byte("some random branch token"))
	s.NoError(err)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_VersionIncrease() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	item := &VersionHistoryItem{
		eventID: 8,
		version: 5,
	}
	err := history.AddOrUpdateItem(item)
	s.NoError(err)

	s.Equal(NewVersionHistory(
		branchToken,
		[]*VersionHistoryItem{
			{eventID: 3, version: 0},
			{eventID: 6, version: 4},
			{eventID: 8, version: 5},
		},
	), history)

}

func (s *versionHistorySuite) TestAddOrUpdateItem_EventIDIncrease() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	item := &VersionHistoryItem{
		eventID: 8,
		version: 4,
	}
	err := history.AddOrUpdateItem(item)
	s.NoError(err)

	s.Equal(NewVersionHistory(
		branchToken,
		[]*VersionHistoryItem{
			{eventID: 3, version: 0},
			{eventID: 8, version: 4},
		},
	), history)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_Failed_LowerVersion() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	err := history.AddOrUpdateItem(NewVersionHistoryItem(8, 3))
	s.Error(err)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_Failed_SameVersion_EventIDNotIncreasing() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	err := history.AddOrUpdateItem(NewVersionHistoryItem(5, 4))
	s.Error(err)

	err = history.AddOrUpdateItem(NewVersionHistoryItem(6, 4))
	s.Error(err)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_Failed_VersionNoIncreasing() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	err := history.AddOrUpdateItem(NewVersionHistoryItem(6, 3))
	s.Error(err)

	err = history.AddOrUpdateItem(NewVersionHistoryItem(2, 3))
	s.Error(err)

	err = history.AddOrUpdateItem(NewVersionHistoryItem(7, 3))
	s.Error(err)
}

func (s *versionHistoriesSuite) TestContainsItem_True() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	prevEventID := common.FirstEventID - 1
	for _, item := range items {
		for eventID := prevEventID + 1; eventID <= item.GetEventID(); eventID++ {
			s.True(history.ContainsItem(NewVersionHistoryItem(eventID, item.GetVersion())))
		}
		prevEventID = item.GetEventID()
	}
}

func (s *versionHistoriesSuite) TestContainsItem_False() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	s.False(history.ContainsItem(NewVersionHistoryItem(4, 0)))
	s.False(history.ContainsItem(NewVersionHistoryItem(3, 1)))

	s.False(history.ContainsItem(NewVersionHistoryItem(7, 4)))
	s.False(history.ContainsItem(NewVersionHistoryItem(6, 5)))
}

func (s *versionHistorySuite) TestIsLCAAppendable_True() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	ret := history.IsLCAAppendable(NewVersionHistoryItem(6, 4))
	s.True(ret)
}

func (s *versionHistorySuite) TestIsLCAAppendable_False_VersionNotMatch() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	ret := history.IsLCAAppendable(NewVersionHistoryItem(6, 7))
	s.False(ret)
}

func (s *versionHistorySuite) TestIsLCAAppendable_False_EventIDNotMatch() {
	branchToken := []byte("some random branch token")
	items := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}
	history := NewVersionHistory(branchToken, items)

	ret := history.IsLCAAppendable(NewVersionHistoryItem(7, 4))
	s.False(ret)
}

func (s *versionHistorySuite) TestFindLCAItem_ReturnLocal() {
	localBranchToken := []byte("local branch token")
	localItems := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 7, version: 4},
		{eventID: 8, version: 8},
		{eventID: 11, version: 12},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	item, err := localVersionHistory.FindLCAItem(remoteVersionHistory)
	s.NoError(err)
	s.Equal(NewVersionHistoryItem(5, 4), item)
}

func (s *versionHistorySuite) TestFindLCAItem_ReturnRemote() {
	localBranchToken := []byte("local branch token")
	localItems := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 6, version: 6},
		{eventID: 11, version: 12},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	item, err := localVersionHistory.FindLCAItem(remoteVersionHistory)
	s.NoError(err)
	s.Equal(NewVersionHistoryItem(6, 6), item)
}

func (s *versionHistorySuite) TestFindLCAItem_Error_NoLCA() {
	localBranchToken := []byte("local branch token")
	localItems := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*VersionHistoryItem{
		{eventID: 3, version: 1},
		{eventID: 7, version: 2},
		{eventID: 8, version: 3},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	_, err := localVersionHistory.FindLCAItem(remoteVersionHistory)
	s.Error(err)
}

func (s *versionHistorySuite) TestGetFirstItem_Success() {
	branchToken := []byte("some random branch token")
	item := NewVersionHistoryItem(3, 0)
	history := NewVersionHistory(branchToken, []*VersionHistoryItem{item})

	firstItem, err := history.GetFirstItem()
	s.NoError(err)
	s.Equal(item, firstItem)

	item = NewVersionHistoryItem(4, 0)
	err = history.AddOrUpdateItem(item)
	s.NoError(err)

	firstItem, err = history.GetFirstItem()
	s.NoError(err)
	s.Equal(item, firstItem)

	err = history.AddOrUpdateItem(NewVersionHistoryItem(7, 1))
	s.NoError(err)

	firstItem, err = history.GetFirstItem()
	s.NoError(err)
	s.Equal(item, firstItem)
}

func (s *versionHistorySuite) TestGetFirstItem_Failure() {
	branchToken := []byte("some random branch token")
	history := NewVersionHistory(branchToken, []*VersionHistoryItem{})

	_, err := history.GetFirstItem()
	s.IsType(&shared.BadRequestError{}, err)
}

func (s *versionHistorySuite) TestGetLastItem_Success() {
	branchToken := []byte("some random branch token")
	item := NewVersionHistoryItem(3, 0)
	history := NewVersionHistory(branchToken, []*VersionHistoryItem{item})

	lastItem, err := history.GetLastItem()
	s.NoError(err)
	s.Equal(item, lastItem)

	item = NewVersionHistoryItem(4, 0)
	err = history.AddOrUpdateItem(item)
	s.NoError(err)

	lastItem, err = history.GetLastItem()
	s.NoError(err)
	s.Equal(item, lastItem)

	item = NewVersionHistoryItem(7, 1)
	err = history.AddOrUpdateItem(item)
	s.NoError(err)

	lastItem, err = history.GetLastItem()
	s.NoError(err)
	s.Equal(item, lastItem)
}

func (s *versionHistorySuite) TestGetLastItem_Failure() {
	branchToken := []byte("some random branch token")
	history := NewVersionHistory(branchToken, []*VersionHistoryItem{})

	_, err := history.GetLastItem()
	s.IsType(&shared.BadRequestError{}, err)
}

func (s *versionHistorySuite) TestEquals() {
	localBranchToken := []byte("local branch token")
	localItems := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*VersionHistoryItem{
		{eventID: 3, version: 1},
		{eventID: 7, version: 2},
		{eventID: 8, version: 3},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	s.False(localVersionHistory.Equals(remoteVersionHistory))
	s.True(localVersionHistory.Equals(localVersionHistory.Duplicate()))
	s.True(remoteVersionHistory.Equals(remoteVersionHistory.Duplicate()))
}

func (s *versionHistoriesSuite) TestConversion() {
	branchToken := []byte("some random branch token")
	localItems := []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	versionHistory := NewVersionHistory(branchToken, localItems)
	histories := NewVersionHistories(versionHistory)

	s.Equal(&VersionHistories{
		currentVersionHistoryIndex: 0,
		histories:                  []*VersionHistory{versionHistory},
	}, histories)

	s.Equal(histories, NewVersionHistoriesFromThrift(histories.ToThrift()))
}

func (s *versionHistoriesSuite) TestAddGetVersionHistory() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 6, version: 6},
		{eventID: 11, version: 12},
	})

	histories := NewVersionHistories(versionHistory1)
	s.Equal(0, histories.currentVersionHistoryIndex)

	currentBranchChanged, newVersionHistoryIndex, err := histories.AddVersionHistory(versionHistory2)
	s.Nil(err)
	s.True(currentBranchChanged)
	s.Equal(1, newVersionHistoryIndex)
	s.Equal(1, histories.currentVersionHistoryIndex)

	resultVersionHistory1, err := histories.GetVersionHistory(0)
	s.Nil(err)
	s.Equal(versionHistory1, resultVersionHistory1)

	resultVersionHistory2, err := histories.GetVersionHistory(1)
	s.Nil(err)
	s.Equal(versionHistory2, resultVersionHistory2)
}

func (s *versionHistoriesSuite) TestFindLCAVersionHistoryIndexAndItem_LargerEventIDWins() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 6, version: 6},
		{eventID: 11, version: 12},
	})

	histories := NewVersionHistories(versionHistory1)
	_, _, err := histories.AddVersionHistory(versionHistory2)
	s.Nil(err)

	versionHistoryIncoming := NewVersionHistory([]byte("branch token incoming"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 8, version: 6},
		{eventID: 11, version: 100},
	})

	index, item, err := histories.FindLCAVersionHistoryIndexAndItem(versionHistoryIncoming)
	s.Nil(err)
	s.Equal(0, index)
	s.Equal(NewVersionHistoryItem(7, 6), item)
}

func (s *versionHistoriesSuite) TestFindLCAVersionHistoryIndexAndItem_SameEventIDShorterLengthWins() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
	})

	histories := NewVersionHistories(versionHistory1)
	_, _, err := histories.AddVersionHistory(versionHistory2)
	s.Nil(err)

	versionHistoryIncoming := NewVersionHistory([]byte("branch token incoming"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 8, version: 6},
		{eventID: 11, version: 100},
	})

	index, item, err := histories.FindLCAVersionHistoryIndexAndItem(versionHistoryIncoming)
	s.Nil(err)
	s.Equal(1, index)
	s.Equal(NewVersionHistoryItem(7, 6), item)
}

func (s *versionHistoriesSuite) TestFindFirstVersionHistoryIndexByItem() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	})

	histories := NewVersionHistories(versionHistory1)
	_, _, err := histories.AddVersionHistory(versionHistory2)
	s.Nil(err)

	index, err := histories.FindFirstVersionHistoryIndexByItem(NewVersionHistoryItem(8, 10))
	s.NoError(err)
	s.Equal(1, index)

	index, err = histories.FindFirstVersionHistoryIndexByItem(NewVersionHistoryItem(4, 4))
	s.NoError(err)
	s.Equal(0, index)

	index, err = histories.FindFirstVersionHistoryIndexByItem(NewVersionHistoryItem(41, 4))
	s.Error(err)
}

func (s *versionHistoriesSuite) TestCurrentVersionHistoryIndexIsInReplay() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 6, version: 6},
		{eventID: 11, version: 12},
	})

	histories := NewVersionHistories(versionHistory1)
	s.Equal(0, histories.currentVersionHistoryIndex)

	currentBranchChanged, newVersionHistoryIndex, err := histories.AddVersionHistory(versionHistory2)
	s.Nil(err)
	s.True(currentBranchChanged)
	s.Equal(1, newVersionHistoryIndex)
	s.Equal(1, histories.currentVersionHistoryIndex)

	isInReplay, err := histories.IsRebuilt()
	s.NoError(err)
	s.False(isInReplay)

	err = histories.SetCurrentVersionHistoryIndex(0)
	s.NoError(err)
	isInReplay, err = histories.IsRebuilt()
	s.NoError(err)
	s.True(isInReplay)

	err = histories.SetCurrentVersionHistoryIndex(1)
	s.NoError(err)
	isInReplay, err = histories.IsRebuilt()
	s.NoError(err)
	s.False(isInReplay)
}
