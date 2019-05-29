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
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"

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
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(items)
	result := history.History
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
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		EventID: 8,
		Version: 5,
	})

	s.NoError(err)
	s.Equal(len(history.History), len(items)+1)
	s.Equal(int64(8), history.History[2].EventID)
	s.Equal(int64(5), history.History[2].Version)
}

func (s *versionHistoryStoreSuite) TestUpdateVersionHistory_UpdateEventID() {
	items := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		EventID: 8,
		Version: 4,
	})

	s.NoError(err)
	s.Equal(len(history.History), len(items))
	s.Equal(int64(8), history.History[1].EventID)
	s.Equal(int64(4), history.History[1].Version)
}

func (s *versionHistoryStoreSuite) TestUpdateVersionHistory_Failed_LowerVersion() {
	items := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		EventID: 8,
		Version: 3,
	})

	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestUpdateVersionHistory_Failed_EventIDNotIncrease() {
	items := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		EventID: 5,
		Version: 4,
	})

	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestUpdateVersionHistory_Failed_EventIDMatch_VersionNotMatch() {
	items := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(items)
	err := history.Update(VersionHistoryItem{
		EventID: 6,
		Version: 7,
	})

	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestIsAppendable_True() {
	items := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(items)
	appendItem := VersionHistoryItem{
		EventID: 6,
		Version: 4,
	}

	s.True(history.IsAppendable(appendItem))
}

func (s *versionHistoryStoreSuite) TestIsAppendable_False_VersionNotMatch() {
	items := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(items)
	appendItem := VersionHistoryItem{
		EventID: 6,
		Version: 7,
	}

	s.False(history.IsAppendable(appendItem))
}

func (s *versionHistoryStoreSuite) TestIsAppendable_False_EventIDNotMatch() {
	items := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 4},
	}

	history := NewVersionHistory(items)
	appendItem := VersionHistoryItem{
		EventID: 7,
		Version: 4,
	}

	s.False(history.IsAppendable(appendItem))
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_ReturnLocal() {
	localItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	remoteItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 7, Version: 4},
		{EventID: 8, Version: 8},
		{EventID: 11, Version: 12},
	}
	local := NewVersionHistory(localItems)
	remote := NewVersionHistory(remoteItems)
	item, err := local.FindLowestCommonVersionHistoryItem(remote)
	s.NoError(err)
	s.Equal(int64(5), item.EventID)
	s.Equal(int64(4), item.Version)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_ReturnRemote() {
	localItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	remoteItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 6, Version: 6},
		{EventID: 11, Version: 12},
	}
	local := NewVersionHistory(localItems)
	remote := NewVersionHistory(remoteItems)
	item, err := local.FindLowestCommonVersionHistoryItem(remote)
	s.NoError(err)
	s.Equal(int64(6), item.EventID)
	s.Equal(int64(6), item.Version)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_Error_NoLCA() {
	localItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	remoteItems := []VersionHistoryItem{
		{EventID: 3, Version: 1},
		{EventID: 7, Version: 2},
		{EventID: 8, Version: 3},
	}
	local := NewVersionHistory(localItems)
	remote := NewVersionHistory(remoteItems)
	_, err := local.FindLowestCommonVersionHistoryItem(remote)
	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_Error_InvalidInput() {
	localItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	remoteItems := []VersionHistoryItem{
		{EventID: 3, Version: 1},
		{EventID: 7, Version: 2},
		{EventID: 6, Version: 3},
	}
	local := NewVersionHistory(localItems)
	remote := NewVersionHistory(remoteItems)
	_, err := local.FindLowestCommonVersionHistoryItem(remote)
	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistoryItem_Error_NilInput() {
	localItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	local := NewVersionHistory(localItems)
	remote := NewVersionHistory([]VersionHistoryItem{{-1, -1}})
	_, err := local.FindLowestCommonVersionHistoryItem(remote)
	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestNewVersionHistories_Panic() {
	expectedPanic := func() { NewVersionHistories([]VersionHistory{}) }
	s.Panics(expectedPanic)
}

func (s *versionHistoryStoreSuite) TestNewVersionHistories() {
	localItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	local := NewVersionHistory(localItems)
	histories := NewVersionHistories([]VersionHistory{local})
	s.NotNil(histories)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistory_UpdateExistingHistory() {
	localItems1 := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	localItems2 := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 8, Version: 4},
		{EventID: 9, Version: 6},
	}
	remoteItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 8, Version: 4},
		{EventID: 10, Version: 6},
		{EventID: 11, Version: 12},
	}
	local1 := NewVersionHistory(localItems1)
	local2 := NewVersionHistory(localItems2)
	remote := NewVersionHistory(remoteItems)
	histories := NewVersionHistories([]VersionHistory{local1, local2})
	item, history, err := histories.FindLowestCommonVersionHistory(remote)
	s.NoError(err)
	s.Equal(history, local2)
	s.Equal(int64(9), item.EventID)
	s.Equal(int64(6), item.Version)

	err = histories.AddHistory(item, history, remote)
	s.NoError(err)
	s.Equal(histories.Histories[1], remote)
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistory_ForkNewHistory() {
	localItems1 := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	localItems2 := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 8, Version: 4},
		{EventID: 9, Version: 6},
	}
	remoteItems := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 6, Version: 7},
		{EventID: 10, Version: 12},
	}
	local1 := NewVersionHistory(localItems1)
	local2 := NewVersionHistory(localItems2)
	remote := NewVersionHistory(remoteItems)
	histories := NewVersionHistories([]VersionHistory{local1, local2})
	item, history, err := histories.FindLowestCommonVersionHistory(remote)
	s.NoError(err)
	s.Equal(int64(3), item.EventID)
	s.Equal(int64(0), item.Version)

	err = histories.AddHistory(item, history, remote)
	s.NoError(err)
	s.Equal(3, len(histories.Histories))
}

func (s *versionHistoryStoreSuite) TestFindLowestCommonVersionHistory_Error() {
	localItems1 := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	localItems2 := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 8, Version: 4},
		{EventID: 9, Version: 6},
	}
	remoteItems := []VersionHistoryItem{
		{EventID: 3, Version: 1},
		{EventID: 6, Version: 7},
		{EventID: 10, Version: 12},
	}
	local1 := NewVersionHistory(localItems1)
	local2 := NewVersionHistory(localItems2)
	remote := NewVersionHistory(remoteItems)
	histories := NewVersionHistories([]VersionHistory{local1, local2})
	_, _, err := histories.FindLowestCommonVersionHistory(remote)
	s.Error(err)
}

func (s *versionHistoryStoreSuite) TestNewVersionHistoriesFromThrift() {
	tHistories := shared.VersionHistories{
		Histories: []*shared.VersionHistory{
			{
				BranchToken: []byte{},
				History: []*shared.VersionHistoryItem{
					{
						EventID: common.Int64Ptr(1),
						Version: common.Int64Ptr(1),
					},
				},
			},
		},
	}

	histories := NewVersionHistoriesFromThrift(&tHistories)
	s.NotNil(histories)
	s.Equal(tHistories.GetHistories()[0].GetHistory()[0].GetEventID(), histories.Histories[0].History[0].EventID)
	s.Equal(tHistories.GetHistories()[0].GetHistory()[0].GetVersion(), histories.Histories[0].History[0].Version)
}

func (s *versionHistoryStoreSuite) TestToThrift() {
	items := []VersionHistoryItem{
		{EventID: 3, Version: 0},
		{EventID: 5, Version: 4},
		{EventID: 7, Version: 6},
		{EventID: 9, Version: 10},
	}
	history := NewVersionHistory(items)
	histories := NewVersionHistories([]VersionHistory{history})
	tHistories := histories.ToThrift()
	s.NotNil(tHistories)
	for idx, item := range items {
		s.Equal(tHistories.GetHistories()[0].GetHistory()[idx].GetEventID(), item.EventID)
		s.Equal(tHistories.GetHistories()[0].GetHistory()[idx].GetVersion(), item.Version)
	}
}
