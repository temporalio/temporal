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
	"github.com/stretchr/testify/suite"
	"testing"
)

type (
	versionHistoryStoreSuite struct {
		suite.Suite
	}
)

func TestVersionHistoryStore(t *testing.T) {
	s := new(versionHistoryStoreSuite)
	suite.Run(t, s)
}

func (s *versionHistoryStoreSuite) TestNewVersionHistory() {
	items := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(items)
	result := history.history
	s.Equal(items, result)
}

func (s *versionHistoryStoreSuite) TestNewVersionHistory_Panic() {
	items := []VersionHistoryItem{}

	expectedPanic := func() {
		NewVersionHistory(items)
	}
	s.Panics(expectedPanic)
}

func (s *versionHistoryStoreSuite) TestUpdateVersionHistory_CreateNewItem() {
	items := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		eventID: 8,
		version: 5,
	})

	s.NoError(err)
	s.Equal(len(history.history), len(items)+1)
	s.Equal(int64(8), history.history[2].eventID)
	s.Equal(int64(5), history.history[2].version)
}

func (s *versionHistoryStoreSuite) TestUpdateVersionHistory_UpdateEventID() {
	items := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		eventID: 8,
		version: 4,
	})

	s.NoError(err)
	s.Equal(len(history.history), len(items))
	s.Equal(int64(8), history.history[1].eventID)
	s.Equal(int64(4), history.history[1].version)
}

func (s *versionHistoryStoreSuite) TestUpdateVersionHistory_Failed_LowerVersion() {
	items := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		eventID: 8,
		version: 3,
	})

	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestUpdateVersionHistory_Failed_EventIDNotIncrease() {
	items := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		eventID: 5,
		version: 4,
	})

	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestUpdateVersionHistory_Failed_EventIDMatch_VersionNotMatch() {
	items := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		eventID: 6,
		version: 7,
	})

	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestIsAppendable_True() {
	items := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(items)
	appendItem := VersionHistoryItem{
		eventID: 6,
		version: 4,
	}

	s.True(history.IsAppendable(appendItem))
}

func (s *versionHistoryStoreSuite) TestIsAppendable_False_VersionNotMatch() {
	items := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(items)
	appendItem := VersionHistoryItem{
		eventID: 6,
		version: 7,
	}

	s.False(history.IsAppendable(appendItem))
}

func (s *versionHistoryStoreSuite) TestIsAppendable_False_EventIDNotMatch() {
	items := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 4},
	}

	history := NewVersionHistory(items)
	appendItem := VersionHistoryItem{
		eventID: 7,
		version: 4,
	}

	s.False(history.IsAppendable(appendItem))
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_ReturnLocal() {
	localItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	remoteItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 7, version: 4},
		{eventID: 8, version: 8},
		{eventID: 11, version: 12},
	}
	local := NewVersionHistory(localItems)
	remote := NewVersionHistory(remoteItems)
	item, err := local.FindLowestCommonVersionHistoryItem(remote)
	s.NoError(err)
	s.Equal(int64(5), item.eventID)
	s.Equal(int64(4), item.version)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_ReturnRemote() {
	localItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	remoteItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 6, version: 6},
		{eventID: 11, version: 12},
	}
	local := NewVersionHistory(localItems)
	remote := NewVersionHistory(remoteItems)
	item, err := local.FindLowestCommonVersionHistoryItem(remote)
	s.NoError(err)
	s.Equal(int64(6), item.eventID)
	s.Equal(int64(6), item.version)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_Error_NoLCA() {
	localItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	remoteItems := []VersionHistoryItem{
		{eventID: 3, version: 1},
		{eventID: 7, version: 2},
		{eventID: 8, version: 3},
	}
	local := NewVersionHistory(localItems)
	remote := NewVersionHistory(remoteItems)
	_, err := local.FindLowestCommonVersionHistoryItem(remote)
	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_Error_InvalidInput() {
	localItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	remoteItems := []VersionHistoryItem{
		{eventID: 3, version: 1},
		{eventID: 7, version: 2},
		{eventID: 6, version: 3},
	}
	local := NewVersionHistory(localItems)
	remote := NewVersionHistory(remoteItems)
	_, err := local.FindLowestCommonVersionHistoryItem(remote)
	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_Error_NilInput() {
	localItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	local := NewVersionHistory(localItems)
	_, err := local.FindLowestCommonVersionHistoryItem(VersionHistory{})
	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestNewVersionHistories_Panic() {
	expectedPanic := func() { NewVersionHistories([]VersionHistory{}) }
	s.Panics(expectedPanic)
}

func (s *versionHistoryStoreSuite) TestNewVersionHistories() {
	localItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	local := NewVersionHistory(localItems)
	histories := NewVersionHistories([]VersionHistory{local})
	s.NotNil(histories)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistory_UpdateExistingHistory() {
	localItems1 := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	localItems2 := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 8, version: 4},
		{eventID: 9, version: 6},
	}
	remoteItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 8, version: 4},
		{eventID: 10, version: 6},
		{eventID: 11, version: 12},
	}
	local1 := NewVersionHistory(localItems1)
	local2 := NewVersionHistory(localItems2)
	remote := NewVersionHistory(remoteItems)
	histories := NewVersionHistories([]VersionHistory{local1, local2})
	item, history, err := histories.FindLowestCommonVersionHistory(remote)
	s.NoError(err)
	s.Equal(history, local2)
	s.Equal(int64(9), item.eventID)
	s.Equal(int64(6), item.version)

	err = histories.AddHistory(item, history, remote)
	s.NoError(err)
	s.Equal(histories.versionHistories[1], remote)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistory_ForkNewHistory() {
	localItems1 := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	localItems2 := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 8, version: 4},
		{eventID: 9, version: 6},
	}
	remoteItems := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 6, version: 7},
		{eventID: 10, version: 12},
	}
	local1 := NewVersionHistory(localItems1)
	local2 := NewVersionHistory(localItems2)
	remote := NewVersionHistory(remoteItems)
	histories := NewVersionHistories([]VersionHistory{local1, local2})
	item, history, err := histories.FindLowestCommonVersionHistory(remote)
	s.NoError(err)
	s.Equal(int64(3), item.eventID)
	s.Equal(int64(0), item.version)

	err = histories.AddHistory(item, history, remote)
	s.NoError(err)
	s.Equal(3, len(histories.versionHistories))
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistory_Error() {
	localItems1 := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 5, version: 4},
		{eventID: 7, version: 6},
		{eventID: 9, version: 10},
	}
	localItems2 := []VersionHistoryItem{
		{eventID: 3, version: 0},
		{eventID: 8, version: 4},
		{eventID: 9, version: 6},
	}
	remoteItems := []VersionHistoryItem{
		{eventID: 3, version: 1},
		{eventID: 6, version: 7},
		{eventID: 10, version: 12},
	}
	local1 := NewVersionHistory(localItems1)
	local2 := NewVersionHistory(localItems2)
	remote := NewVersionHistory(remoteItems)
	histories := NewVersionHistories([]VersionHistory{local1, local2})
	_, _, err := histories.FindLowestCommonVersionHistory(remote)
	s.Error(err)
}
