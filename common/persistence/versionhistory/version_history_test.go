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

package versionhistory

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common"
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

func (s *versionHistorySuite) TestDuplicateUntilLCAItem_Success() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}

	history := NewVersionHistory(BranchToken, Items)

	newHistory, err := CopyVersionHistoryUntilLCAVersionHistoryItem(history, NewVersionHistoryItem(2, 0))
	s.NoError(err)
	newBranchToken := []byte("other random branch token")
	SetVersionHistoryBranchToken(newHistory, newBranchToken)
	s.Equal(newBranchToken, newHistory.BranchToken)
	s.Equal(NewVersionHistory(
		newBranchToken,
		[]*historyspb.VersionHistoryItem{{EventId: 2, Version: 0}},
	), newHistory)

	newHistory, err = CopyVersionHistoryUntilLCAVersionHistoryItem(history, NewVersionHistoryItem(5, 4))
	s.NoError(err)
	newBranchToken = []byte("another random branch token")
	SetVersionHistoryBranchToken(newHistory, newBranchToken)
	s.Equal(newBranchToken, newHistory.BranchToken)
	s.Equal(NewVersionHistory(
		newBranchToken,
		[]*historyspb.VersionHistoryItem{
			{EventId: 3, Version: 0},
			{EventId: 5, Version: 4},
		},
	), newHistory)

	newHistory, err = CopyVersionHistoryUntilLCAVersionHistoryItem(history, NewVersionHistoryItem(6, 4))
	s.NoError(err)
	newBranchToken = []byte("yet another random branch token")
	SetVersionHistoryBranchToken(newHistory, newBranchToken)
	s.Equal(newBranchToken, newHistory.BranchToken)
	s.Equal(NewVersionHistory(
		newBranchToken,
		[]*historyspb.VersionHistoryItem{
			{EventId: 3, Version: 0},
			{EventId: 6, Version: 4},
		},
	), newHistory)
}

func (s *versionHistorySuite) TestDuplicateUntilLCAItem_Failure() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}

	history := NewVersionHistory(BranchToken, Items)

	_, err := CopyVersionHistoryUntilLCAVersionHistoryItem(history, NewVersionHistoryItem(4, 0))
	s.IsType(&serviceerror.InvalidArgument{}, err)

	_, err = CopyVersionHistoryUntilLCAVersionHistoryItem(history, NewVersionHistoryItem(2, 1))
	s.IsType(&serviceerror.InvalidArgument{}, err)

	_, err = CopyVersionHistoryUntilLCAVersionHistoryItem(history, NewVersionHistoryItem(5, 3))
	s.IsType(&serviceerror.InvalidArgument{}, err)

	_, err = CopyVersionHistoryUntilLCAVersionHistoryItem(history, NewVersionHistoryItem(7, 5))
	s.IsType(&serviceerror.InvalidArgument{}, err)

	_, err = CopyVersionHistoryUntilLCAVersionHistoryItem(history, NewVersionHistoryItem(4, 0))
	s.IsType(&serviceerror.InvalidArgument{}, err)

	_, err = CopyVersionHistoryUntilLCAVersionHistoryItem(history, NewVersionHistoryItem(7, 4))
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *versionHistorySuite) TestSetBranchToken() {
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(nil, Items)

	SetVersionHistoryBranchToken(history, []byte("some random branch token"))
}

func (s *versionHistorySuite) TestAddOrUpdateItem_VersionIncrease() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	item := &historyspb.VersionHistoryItem{
		EventId: 8,
		Version: 5,
	}
	err := AddOrUpdateVersionHistoryItem(history, item)
	s.NoError(err)

	s.Equal(NewVersionHistory(
		BranchToken,
		[]*historyspb.VersionHistoryItem{
			{EventId: 3, Version: 0},
			{EventId: 6, Version: 4},
			{EventId: 8, Version: 5},
		},
	), history)

}

func (s *versionHistorySuite) TestAddOrUpdateItem_EventIDIncrease() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	item := &historyspb.VersionHistoryItem{
		EventId: 8,
		Version: 4,
	}
	err := AddOrUpdateVersionHistoryItem(history, item)
	s.NoError(err)

	s.Equal(NewVersionHistory(
		BranchToken,
		[]*historyspb.VersionHistoryItem{
			{EventId: 3, Version: 0},
			{EventId: 8, Version: 4},
		},
	), history)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_Failed_LowerVersion() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	err := AddOrUpdateVersionHistoryItem(history, NewVersionHistoryItem(8, 3))
	s.Error(err)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_Failed_SameVersion_EventIDNotIncreasing() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	err := AddOrUpdateVersionHistoryItem(history, NewVersionHistoryItem(5, 4))
	s.Error(err)

	err = AddOrUpdateVersionHistoryItem(history, NewVersionHistoryItem(6, 4))
	s.Error(err)
}

func (s *versionHistorySuite) TestAddOrUpdateItem_Failed_VersionNoIncreasing() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	err := AddOrUpdateVersionHistoryItem(history, NewVersionHistoryItem(6, 3))
	s.Error(err)

	err = AddOrUpdateVersionHistoryItem(history, NewVersionHistoryItem(2, 3))
	s.Error(err)

	err = AddOrUpdateVersionHistoryItem(history, NewVersionHistoryItem(7, 3))
	s.Error(err)
}

func (s *versionHistoriesSuite) TestContainsItem_True() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	prevEventID := common.FirstEventID - 1
	for _, item := range Items {
		for EventID := prevEventID + 1; EventID <= item.GetEventId(); EventID++ {
			s.True(ContainsVersionHistoryItem(history, NewVersionHistoryItem(EventID, item.GetVersion())))
		}
		prevEventID = item.GetEventId()
	}
}

func (s *versionHistoriesSuite) TestContainsItem_False() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	s.False(ContainsVersionHistoryItem(history, NewVersionHistoryItem(4, 0)))
	s.False(ContainsVersionHistoryItem(history, NewVersionHistoryItem(3, 1)))

	s.False(ContainsVersionHistoryItem(history, NewVersionHistoryItem(7, 4)))
	s.False(ContainsVersionHistoryItem(history, NewVersionHistoryItem(6, 5)))
}

func (s *versionHistorySuite) TestIsLCAAppendable_True() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	ret := IsLCAVersionHistoryItemAppendable(history, NewVersionHistoryItem(6, 4))
	s.True(ret)
}

func (s *versionHistorySuite) TestIsLCAAppendable_False_VersionNotMatch() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	ret := IsLCAVersionHistoryItemAppendable(history, NewVersionHistoryItem(6, 7))
	s.False(ret)
}

func (s *versionHistorySuite) TestIsLCAAppendable_False_EventIDNotMatch() {
	BranchToken := []byte("some random branch token")
	Items := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 6, Version: 4},
	}
	history := NewVersionHistory(BranchToken, Items)

	ret := IsLCAVersionHistoryItemAppendable(history, NewVersionHistoryItem(7, 4))
	s.False(ret)
}

func (s *versionHistorySuite) TestFindLCAItem_ReturnLocal() {
	localBranchToken := []byte("local branch token")
	localItems := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
		{EventId: 9, Version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 7, Version: 4},
		{EventId: 8, Version: 8},
		{EventId: 11, Version: 12},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	item, err := FindLCAVersionHistoryItem(localVersionHistory, remoteVersionHistory)
	s.NoError(err)
	s.Equal(NewVersionHistoryItem(5, 4), item)
}

func (s *versionHistorySuite) TestFindLCAItem_ReturnRemote() {
	localBranchToken := []byte("local branch token")
	localItems := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
		{EventId: 9, Version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 6, Version: 6},
		{EventId: 11, Version: 12},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	item, err := FindLCAVersionHistoryItem(localVersionHistory, remoteVersionHistory)
	s.NoError(err)
	s.Equal(NewVersionHistoryItem(6, 6), item)
}

func (s *versionHistorySuite) TestFindLCAItem_Error_NoLCA() {
	localBranchToken := []byte("local branch token")
	localItems := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
		{EventId: 9, Version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 1},
		{EventId: 7, Version: 2},
		{EventId: 8, Version: 3},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	_, err := FindLCAVersionHistoryItem(localVersionHistory, remoteVersionHistory)
	s.Error(err)
}

func (s *versionHistorySuite) TestGetFirstItem_Success() {
	BranchToken := []byte("some random branch token")
	item := NewVersionHistoryItem(3, 0)
	history := NewVersionHistory(BranchToken, []*historyspb.VersionHistoryItem{item})

	firstItem, err := GetFirstVersionHistoryItem(history)
	s.NoError(err)
	s.Equal(item, firstItem)

	item = NewVersionHistoryItem(4, 0)
	err = AddOrUpdateVersionHistoryItem(history, item)
	s.NoError(err)

	firstItem, err = GetFirstVersionHistoryItem(history)
	s.NoError(err)
	s.Equal(item, firstItem)

	err = AddOrUpdateVersionHistoryItem(history, NewVersionHistoryItem(7, 1))
	s.NoError(err)

	firstItem, err = GetFirstVersionHistoryItem(history)
	s.NoError(err)
	s.Equal(item, firstItem)
}

func (s *versionHistorySuite) TestGetFirstItem_Failure() {
	BranchToken := []byte("some random branch token")
	history := NewVersionHistory(BranchToken, []*historyspb.VersionHistoryItem{})

	_, err := GetFirstVersionHistoryItem(history)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *versionHistorySuite) TestGetLastItem_Success() {
	BranchToken := []byte("some random branch token")
	item := NewVersionHistoryItem(3, 0)
	history := NewVersionHistory(BranchToken, []*historyspb.VersionHistoryItem{item})

	lastItem, err := GetLastVersionHistoryItem(history)
	s.NoError(err)
	s.Equal(item, lastItem)

	item = NewVersionHistoryItem(4, 0)
	err = AddOrUpdateVersionHistoryItem(history, item)
	s.NoError(err)

	lastItem, err = GetLastVersionHistoryItem(history)
	s.NoError(err)
	s.Equal(item, lastItem)

	item = NewVersionHistoryItem(7, 1)
	err = AddOrUpdateVersionHistoryItem(history, item)
	s.NoError(err)

	lastItem, err = GetLastVersionHistoryItem(history)
	s.NoError(err)
	s.Equal(item, lastItem)
}

func (s *versionHistorySuite) TestGetLastItem_Failure() {
	BranchToken := []byte("some random branch token")
	history := NewVersionHistory(BranchToken, []*historyspb.VersionHistoryItem{})

	_, err := GetLastVersionHistoryItem(history)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *versionHistoriesSuite) TestGetVersion_Success() {
	BranchToken := []byte("some random branch token")
	item1 := NewVersionHistoryItem(3, 0)
	item2 := NewVersionHistoryItem(6, 8)
	item3 := NewVersionHistoryItem(8, 12)
	history := NewVersionHistory(BranchToken, []*historyspb.VersionHistoryItem{item1, item2, item3})

	Version, err := GetVersionHistoryEventVersion(history, 1)
	s.NoError(err)
	s.Equal(item1.GetVersion(), Version)
	Version, err = GetVersionHistoryEventVersion(history, 2)
	s.NoError(err)
	s.Equal(item1.GetVersion(), Version)
	Version, err = GetVersionHistoryEventVersion(history, 3)
	s.NoError(err)
	s.Equal(item1.GetVersion(), Version)

	Version, err = GetVersionHistoryEventVersion(history, 4)
	s.NoError(err)
	s.Equal(item2.GetVersion(), Version)
	Version, err = GetVersionHistoryEventVersion(history, 5)
	s.NoError(err)
	s.Equal(item2.GetVersion(), Version)
	Version, err = GetVersionHistoryEventVersion(history, 6)
	s.NoError(err)
	s.Equal(item2.GetVersion(), Version)

	Version, err = GetVersionHistoryEventVersion(history, 7)
	s.NoError(err)
	s.Equal(item3.GetVersion(), Version)
	Version, err = GetVersionHistoryEventVersion(history, 8)
	s.NoError(err)
	s.Equal(item3.GetVersion(), Version)
}

func (s *versionHistoriesSuite) TestGetVersion_Failure() {
	BranchToken := []byte("some random branch token")
	item1 := NewVersionHistoryItem(3, 0)
	item2 := NewVersionHistoryItem(6, 8)
	item3 := NewVersionHistoryItem(8, 12)
	history := NewVersionHistory(BranchToken, []*historyspb.VersionHistoryItem{item1, item2, item3})

	_, err := GetVersionHistoryEventVersion(history, 0)
	s.Error(err)

	_, err = GetVersionHistoryEventVersion(history, 9)
	s.Error(err)
}

func (s *versionHistorySuite) TestEquals() {
	localBranchToken := []byte("local branch token")
	localItems := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
		{EventId: 9, Version: 10},
	}
	remoteBranchToken := []byte("remote branch token")
	remoteItems := []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 1},
		{EventId: 7, Version: 2},
		{EventId: 8, Version: 3},
	}
	localVersionHistory := NewVersionHistory(localBranchToken, localItems)
	remoteVersionHistory := NewVersionHistory(remoteBranchToken, remoteItems)

	s.False(localVersionHistory.Equal(remoteVersionHistory))
	s.True(localVersionHistory.Equal(CopyVersionHistory(localVersionHistory)))
	s.True(remoteVersionHistory.Equal(CopyVersionHistory(remoteVersionHistory)))
}

func (s *versionHistoriesSuite) TestAddGetVersionHistory() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
		{EventId: 9, Version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 6, Version: 6},
		{EventId: 11, Version: 12},
	})

	histories := NewVersionHistories(versionHistory1)
	s.Equal(int32(0), histories.CurrentVersionHistoryIndex)

	currentBranchChanged, newVersionHistoryIndex, err := AddVersionHistory(histories, versionHistory2)
	s.Nil(err)
	s.True(currentBranchChanged)
	s.Equal(int32(1), newVersionHistoryIndex)
	s.Equal(int32(1), histories.CurrentVersionHistoryIndex)

	resultVersionHistory1, err := GetVersionHistory(histories, 0)
	s.Nil(err)
	s.Equal(versionHistory1, resultVersionHistory1)

	resultVersionHistory2, err := GetVersionHistory(histories, 1)
	s.Nil(err)
	s.Equal(versionHistory2, resultVersionHistory2)
}

func (s *versionHistoriesSuite) TestFindLCAVersionHistoryIndexAndItem_LargerEventIDWins() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
		{EventId: 9, Version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 6, Version: 6},
		{EventId: 11, Version: 12},
	})

	histories := NewVersionHistories(versionHistory1)
	_, _, err := AddVersionHistory(histories, versionHistory2)
	s.Nil(err)

	versionHistoryIncoming := NewVersionHistory([]byte("branch token incoming"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 8, Version: 6},
		{EventId: 11, Version: 100},
	})

	item, index, err := FindLCAVersionHistoryItemAndIndex(histories, versionHistoryIncoming)
	s.Nil(err)
	s.Equal(int32(0), index)
	s.Equal(NewVersionHistoryItem(7, 6), item)
}

func (s *versionHistoriesSuite) TestFindLCAVersionHistoryIndexAndItem_SameEventIDShorterLengthWins() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
		{EventId: 9, Version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
	})

	histories := NewVersionHistories(versionHistory1)
	_, _, err := AddVersionHistory(histories, versionHistory2)
	s.Nil(err)

	versionHistoryIncoming := NewVersionHistory([]byte("branch token incoming"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 8, Version: 6},
		{EventId: 11, Version: 100},
	})

	item, index, err := FindLCAVersionHistoryItemAndIndex(histories, versionHistoryIncoming)
	s.Nil(err)
	s.Equal(int32(1), index)
	s.Equal(NewVersionHistoryItem(7, 6), item)
}

func (s *versionHistoriesSuite) TestFindFirstVersionHistoryIndexByItem() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
		{EventId: 9, Version: 10},
	})

	histories := NewVersionHistories(versionHistory1)
	_, _, err := AddVersionHistory(histories, versionHistory2)
	s.Nil(err)

	index, err := FindFirstVersionHistoryIndexByVersionHistoryItem(histories, NewVersionHistoryItem(8, 10))
	s.NoError(err)
	s.Equal(int32(1), index)

	index, err = FindFirstVersionHistoryIndexByVersionHistoryItem(histories, NewVersionHistoryItem(4, 4))
	s.NoError(err)
	s.Equal(int32(0), index)

	_, err = FindFirstVersionHistoryIndexByVersionHistoryItem(histories, NewVersionHistoryItem(41, 4))
	s.Error(err)
}

func (s *versionHistoriesSuite) TestCurrentVersionHistoryIndexIsInReplay() {
	versionHistory1 := NewVersionHistory([]byte("branch token 1"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 7, Version: 6},
		{EventId: 9, Version: 10},
	})
	versionHistory2 := NewVersionHistory([]byte("branch token 2"), []*historyspb.VersionHistoryItem{
		{EventId: 3, Version: 0},
		{EventId: 5, Version: 4},
		{EventId: 6, Version: 6},
		{EventId: 11, Version: 12},
	})

	histories := NewVersionHistories(versionHistory1)
	s.Equal(int32(0), histories.CurrentVersionHistoryIndex)

	currentBranchChanged, newVersionHistoryIndex, err := AddVersionHistory(histories, versionHistory2)
	s.Nil(err)
	s.True(currentBranchChanged)
	s.Equal(int32(1), newVersionHistoryIndex)
	s.Equal(int32(1), histories.CurrentVersionHistoryIndex)

	isInReplay, err := IsVersionHistoriesRebuilt(histories)
	s.NoError(err)
	s.False(isInReplay)

	err = SetCurrentVersionHistoryIndex(histories, 0)
	s.NoError(err)
	isInReplay, err = IsVersionHistoriesRebuilt(histories)
	s.NoError(err)
	s.True(isInReplay)

	err = SetCurrentVersionHistoryIndex(histories, 1)
	s.NoError(err)
	isInReplay, err = IsVersionHistoriesRebuilt(histories)
	s.NoError(err)
	s.False(isInReplay)
}
