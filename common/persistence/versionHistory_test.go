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
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(BranchToken, Items)
	s.Equal(&VersionHistory{
		BranchToken: BranchToken,
		Items:       Items,
	}, history)

	s.Equal(history, NewVersionHistoryFromThrift(history.ToThrift()))
}

func (s *versionHistorySuite) TestDuplicateUntilLCAItem_Success() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(BranchToken, Items)

	newHistory, err := history.DuplicateUntilLCAItem(NewVersionHistoryItem(2, 0))
	s.NoError(err)
	newBranchToken := []byte("other random branch token")
	err = newHistory.SetBranchToken(newBranchToken)
	s.NoError(err)
	s.Equal(newBranchToken, newHistory.GetBranchToken())
	s.Equal(NewVersionHistory(
		newBranchToken,
		[]*VersionHistoryItem{{EventID: 2, Version: 0}},
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
			{EventID: 3, Version: 0},
			{EventID: 5, Version: 4},
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
			{EventID: 3, Version: 0},
			{EventID: 6, Version: 4},
		},
	), newHistory)
}

func (s *versionHistorySuite) TestDuplicateUntilLCAItem_Failure() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(BranchToken, Items)

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
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(nil, Items)

	err := history.SetBranchToken([]byte("some random branch token"))
	s.NoError(err)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_VersionIncrease() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	item := &VersionHistoryItem{
		EventID: 8,
		Version: 5,
	}
	err := history.AddOrUpdateItem(item)
	s.NoError(err)

	s.Equal(NewVersionHistory(
		BranchToken,
		[]*VersionHistoryItem{
			{EventID: 3, Version: 0},
			{EventID: 6, Version: 4},
			{EventID: 8, Version: 5},
		},
	), history)

}

func (s *versionHistorySuite) TestAddOrUpdateItem_EventIDIncrease() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	item := &VersionHistoryItem{
		EventID: 8,
		Version: 4,
	}
	err := history.AddOrUpdateItem(item)
	s.NoError(err)

	s.Equal(NewVersionHistory(
		BranchToken,
		[]*VersionHistoryItem{
			{EventID: 3, Version: 0},
			{EventID: 8, Version: 4},
		},
	), history)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_Failed_LowerVersion() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	err := history.AddOrUpdateItem(NewVersionHistoryItem(8, 3))
	s.Error(err)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_Failed_SameVersion_EventIDNotIncreasing() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	err := history.AddOrUpdateItem(NewVersionHistoryItem(5, 4))
	s.Error(err)

	err = history.AddOrUpdateItem(NewVersionHistoryItem(6, 4))
	s.Error(err)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_Failed_VersionNoIncreasing() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	err := history.AddOrUpdateItem(NewVersionHistoryItem(6, 3))
	s.Error(err)

	err = history.AddOrUpdateItem(NewVersionHistoryItem(2, 3))
	s.Error(err)

	err = history.AddOrUpdateItem(NewVersionHistoryItem(7, 3))
	s.Error(err)
}

func (s *versionHistoriesSuite) TestContainsItem_True() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	prevEventID := common.FirstEventID - 1
	for _, item := range Items {
		for EventID := prevEventID + 1; EventID <= item.GetEventID(); EventID++ {
			s.True(history.ContainsItem(NewVersionHistoryItem(EventID, item.GetVersion())))
		}
		prevEventID = item.GetEventID()
	}
}

func (s *versionHistoriesSuite) TestContainsItem_False() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	s.False(history.ContainsItem(NewVersionHistoryItem(4, 0)))
	s.False(history.ContainsItem(NewVersionHistoryItem(3, 1)))

	s.False(history.ContainsItem(NewVersionHistoryItem(7, 4)))
	s.False(history.ContainsItem(NewVersionHistoryItem(6, 5)))
}

func (s *versionHistorySuite) TestIsLCAAppendable_True() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	ret := history.IsLCAAppendable(NewVersionHistoryItem(6, 4))
	s.True(ret)
}

func (s *versionHistorySuite) TestIsLCAAppendable_False_VersionNotMatch() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	ret := history.IsLCAAppendable(NewVersionHistoryItem(6, 7))
	s.False(ret)
}

func (s *versionHistorySuite) TestIsLCAAppendable_False_EventIDNotMatch() {
	BranchToken := []byte("some random branch token")
	Items := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	ret := history.IsLCAAppendable(NewVersionHistoryItem(7, 4))
	s.False(ret)
}

func (s *versionHistorySuite) TestFindLCAItem_ReturnLocal() {
	localBranchToken := []byte("local branch token")
	localItems := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 7, Version: 4},
		{EventID: 8, Version: 8},
		{EventID: 11, Version: 12},
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
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 6, Version: 6},
		{EventID: 11, Version: 12},
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
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*VersionHistoryItem{
		{EventID: 3, Version: 1},
		{EventID: 7, Version: 2},
		{EventID: 8, Version: 3},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	_, err := localVersionHistory.FindLCAItem(remoteVersionHistory)
	s.Error(err)
}

func (s *versionHistorySuite) TestGetFirstItem_Success() {
	BranchToken := []byte("some random branch token")
	item := NewVersionHistoryItem(3, 0)
	history := NewVersionHistory(BranchToken, []*VersionHistoryItem{item})

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
	BranchToken := []byte("some random branch token")
	history := NewVersionHistory(BranchToken, []*VersionHistoryItem{})

	_, err := history.GetFirstItem()
	s.IsType(&shared.BadRequestError{}, err)
}

func (s *versionHistorySuite) TestGetLastItem_Success() {
	BranchToken := []byte("some random branch token")
	item := NewVersionHistoryItem(3, 0)
	history := NewVersionHistory(BranchToken, []*VersionHistoryItem{item})

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
	BranchToken := []byte("some random branch token")
	history := NewVersionHistory(BranchToken, []*VersionHistoryItem{})

	_, err := history.GetLastItem()
	s.IsType(&shared.BadRequestError{}, err)
}

func (s *versionHistoriesSuite) TestGetVersion_Success() {
	BranchToken := []byte("some random branch token")
	item1 := NewVersionHistoryItem(3, 0)
	item2 := NewVersionHistoryItem(6, 8)
	item3 := NewVersionHistoryItem(8, 12)
	history := NewVersionHistory(BranchToken, []*VersionHistoryItem{item1, item2, item3})

	Version, err := history.GetEventVersion(1)
	s.NoError(err)
	s.Equal(item1.GetVersion(), Version)
	Version, err = history.GetEventVersion(2)
	s.NoError(err)
	s.Equal(item1.GetVersion(), Version)
	Version, err = history.GetEventVersion(3)
	s.NoError(err)
	s.Equal(item1.GetVersion(), Version)

	Version, err = history.GetEventVersion(4)
	s.NoError(err)
	s.Equal(item2.GetVersion(), Version)
	Version, err = history.GetEventVersion(5)
	s.NoError(err)
	s.Equal(item2.GetVersion(), Version)
	Version, err = history.GetEventVersion(6)
	s.NoError(err)
	s.Equal(item2.GetVersion(), Version)

	Version, err = history.GetEventVersion(7)
	s.NoError(err)
	s.Equal(item3.GetVersion(), Version)
	Version, err = history.GetEventVersion(8)
	s.NoError(err)
	s.Equal(item3.GetVersion(), Version)
}

func (s *versionHistoriesSuite) TestGetVersion_Failure() {
	BranchToken := []byte("some random branch token")
	item1 := NewVersionHistoryItem(3, 0)
	item2 := NewVersionHistoryItem(6, 8)
	item3 := NewVersionHistoryItem(8, 12)
	history := NewVersionHistory(BranchToken, []*VersionHistoryItem{item1, item2, item3})

	_, err := history.GetEventVersion(0)
	s.Error(err)

	_, err = history.GetEventVersion(9)
	s.Error(err)
}

func (s *versionHistorySuite) TestEquals() {
	localBranchToken := []byte("local branch token")
	localItems := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*VersionHistoryItem{
		{EventID: 3, Version: 1},
		{EventID: 7, Version: 2},
		{EventID: 8, Version: 3},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	s.False(localVersionHistory.Equals(remoteVersionHistory))
	s.True(localVersionHistory.Equals(localVersionHistory.Duplicate()))
	s.True(remoteVersionHistory.Equals(remoteVersionHistory.Duplicate()))
}

func (s *versionHistoriesSuite) TestConversion() {
	BranchToken := []byte("some random branch token")
	localItems := []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	versionHistory := NewVersionHistory(BranchToken, localItems)
	histories := NewVersionHistories(versionHistory)

	s.Equal(&VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories:                  []*VersionHistory{versionHistory},
	}, histories)

	s.Equal(histories, NewVersionHistoriesFromThrift(histories.ToThrift()))
}

func (s *versionHistoriesSuite) TestAddGetVersionHistory() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 6, Version: 6},
		{EventID: 11, Version: 12},
	})

	histories := NewVersionHistories(versionHistory1)
	s.Equal(0, histories.CurrentVersionHistoryIndex)

	currentBranchChanged, newVersionHistoryIndex, err := histories.AddVersionHistory(versionHistory2)
	s.Nil(err)
	s.True(currentBranchChanged)
	s.Equal(1, newVersionHistoryIndex)
	s.Equal(1, histories.CurrentVersionHistoryIndex)

	resultVersionHistory1, err := histories.GetVersionHistory(0)
	s.Nil(err)
	s.Equal(versionHistory1, resultVersionHistory1)

	resultVersionHistory2, err := histories.GetVersionHistory(1)
	s.Nil(err)
	s.Equal(versionHistory2, resultVersionHistory2)
}

func (s *versionHistoriesSuite) TestFindLCAVersionHistoryIndexAndItem_LargerEventIDWins() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 6, Version: 6},
		{EventID: 11, Version: 12},
	})

	histories := NewVersionHistories(versionHistory1)
	_, _, err := histories.AddVersionHistory(versionHistory2)
	s.Nil(err)

	versionHistoryIncoming := NewVersionHistory([]byte("branch token incoming"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 8, Version: 6},
		{EventID: 11, Version: 100},
	})

	index, item, err := histories.FindLCAVersionHistoryIndexAndItem(versionHistoryIncoming)
	s.Nil(err)
	s.Equal(0, index)
	s.Equal(NewVersionHistoryItem(7, 6), item)
}

func (s *versionHistoriesSuite) TestFindLCAVersionHistoryIndexAndItem_SameEventIDShorterLengthWins() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
	})

	histories := NewVersionHistories(versionHistory1)
	_, _, err := histories.AddVersionHistory(versionHistory2)
	s.Nil(err)

	versionHistoryIncoming := NewVersionHistory([]byte("branch token incoming"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 8, Version: 6},
		{EventID: 11, Version: 100},
	})

	index, item, err := histories.FindLCAVersionHistoryIndexAndItem(versionHistoryIncoming)
	s.Nil(err)
	s.Equal(1, index)
	s.Equal(NewVersionHistoryItem(7, 6), item)
}

func (s *versionHistoriesSuite) TestFindFirstVersionHistoryIndexByItem() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
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

	_, err = histories.FindFirstVersionHistoryIndexByItem(NewVersionHistoryItem(41, 4))
	s.Error(err)
}

func (s *versionHistoriesSuite) TestCurrentVersionHistoryIndexIsInReplay() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 6, Version: 6},
		{EventID: 11, Version: 12},
	})

	histories := NewVersionHistories(versionHistory1)
	s.Equal(0, histories.CurrentVersionHistoryIndex)

	currentBranchChanged, newVersionHistoryIndex, err := histories.AddVersionHistory(versionHistory2)
	s.Nil(err)
	s.True(currentBranchChanged)
	s.Equal(1, newVersionHistoryIndex)
	s.Equal(1, histories.CurrentVersionHistoryIndex)

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
