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
	"fmt"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"reflect"
)

// NewVersionHistory initializes new version history
func NewVersionHistory(items []VersionHistoryItem) VersionHistory {
	if len(items) == 0 {
		panic("version history items cannot be empty")
	}

	newHistoryItems := make([]VersionHistoryItem, len(items))
	copy(newHistoryItems, items)

	return VersionHistory{
		History: newHistoryItems,
	}
}

// Update updates the versionHistory slice
func (v *VersionHistory) Update(item VersionHistoryItem) error {
	if len(v.History) == 0 {
		panic("version history cannot be empty")
	}

	currentItem := item
	lastItem := &v.History[len(v.History)-1]
	if currentItem.Version < lastItem.Version {
		return &shared.BadRequestError{
			Message: fmt.Sprintf("cannot update version history with a lower version %v. Last version: %v",
				currentItem.Version,
				lastItem.Version),
		}
	}

	if currentItem.EventID <= lastItem.EventID {
		return &shared.BadRequestError{
			Message: fmt.Sprintf("cannot add version history with a lower event id %v. Last event id: %v",
				currentItem.EventID,
				lastItem.EventID),
		}
	}

	if currentItem.Version > lastItem.Version {
		// Add a new history
		v.History = append(v.History, currentItem)
	} else {
		// item.version == lastItem.version && item.EventID > lastItem.EventID
		// Update event  id
		lastItem.EventID = currentItem.EventID
	}
	return nil
}

// FindLowestCommonVersionHistoryItem returns the lowest version history item with the same version
func (v *VersionHistory) FindLowestCommonVersionHistoryItem(remote VersionHistory) (VersionHistoryItem, error) {
	localIdx := len(v.History) - 1
	remoteIdx := len(remote.History) - 1

	for localIdx >= 0 && remoteIdx >= 0 {
		localVersionItem := v.History[localIdx]
		remoteVersionItem := remote.History[remoteIdx]
		if localVersionItem.Version == remoteVersionItem.Version {
			if localVersionItem.EventID > remoteVersionItem.EventID {
				return remoteVersionItem, nil
			}
			return localVersionItem, nil
		} else if localVersionItem.Version > remoteVersionItem.Version {
			localIdx--
		} else {
			// localVersionItem.version < remoteVersionItem.version
			remoteIdx--
		}
	}
	return VersionHistoryItem{}, &shared.BadRequestError{
		Message: fmt.Sprintf("version history is malformed. No joint point found."),
	}
}

// IsAppendable checks if a version history item is appendable
func (v *VersionHistory) IsAppendable(item VersionHistoryItem) bool {
	if len(v.History) == 0 {
		panic("version history cannot be empty")
	}
	return v.History[len(v.History)-1] == item
}

// ToThrift return thrift format of version history
func (v *VersionHistory) ToThrift() *shared.VersionHistory {
	tHistory := &shared.VersionHistory{BranchToken: append([]byte(nil), v.BranchToken...)}
	for _, item := range v.History {
		tHistory.History = append(tHistory.History,
			&shared.VersionHistoryItem{EventID: common.Int64Ptr(item.EventID), Version: common.Int64Ptr(item.Version)})
	}
	return tHistory
}

// NewVersionHistories initialize new version histories
func NewVersionHistories(histories []VersionHistory) VersionHistories {
	if len(histories) == 0 {
		panic("version histories cannot be empty")
	}
	newHistories := make([]VersionHistory, len(histories))
	copy(newHistories, histories)
	return VersionHistories{
		Histories: newHistories,
	}
}

// NewVersionHistoriesFromThrift initialize VersionHistories from thrift format
func NewVersionHistoriesFromThrift(thrift *shared.VersionHistories) *VersionHistories {
	if thrift == nil {
		return nil
	}
	histories := VersionHistories{}
	histories.CurrentBranch = thrift.GetCurrentBranch()
	for _, tHistory := range thrift.Histories {
		history := VersionHistory{BranchToken: tHistory.GetBranchToken()}
		for _, item := range tHistory.GetHistory() {
			history.History = append(history.History, VersionHistoryItem{EventID: item.GetEventID(), Version: item.GetVersion()})
		}
		histories.Histories = append(histories.Histories, history)
	}
	return &histories
}

// FindLowestCommonVersionHistory finds the lowest common version history item among all version histories
func (h *VersionHistories) FindLowestCommonVersionHistory(history VersionHistory) (VersionHistoryItem, VersionHistory, error) {
	var versionHistoryItem VersionHistoryItem
	var versionHistory VersionHistory
	for _, localHistory := range h.Histories {
		item, err := localHistory.FindLowestCommonVersionHistoryItem(history)
		if err != nil {
			return versionHistoryItem, versionHistory, err
		}

		if item.EventID > versionHistoryItem.EventID {
			versionHistoryItem = item
			versionHistory = localHistory
		}
	}
	return versionHistoryItem, versionHistory, nil
}

// AddHistory add new history into version histories
// TODO: merge this func with FindLowestCommonVersionHistory
func (h *VersionHistories) AddHistory(item VersionHistoryItem, local VersionHistory, remote VersionHistory) error {
	if local.IsAppendable(item) {
		//it won't update h.versionHistories
		for idx, history := range h.Histories {
			if reflect.DeepEqual(history, local) {
				h.Histories[idx] = remote
			}
		}
	} else {
		h.Histories = append(h.Histories, remote)
	}
	return nil
}

// ToThrift return thrift format of version histories
func (h *VersionHistories) ToThrift() *shared.VersionHistories {
	tHistories := &shared.VersionHistories{}
	tHistories.CurrentBranch = common.Int32Ptr(h.CurrentBranch)
	for _, history := range h.Histories {
		tHistories.Histories = append(tHistories.Histories, history.ToThrift())
	}

	return tHistories
}
