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
	"reflect"
)

type (
	// VersionHistoryItem contains the event id and the associated version
	VersionHistoryItem struct {
		eventID int64
		version int64
	}

	// VersionHistory provides operations on version history
	VersionHistory struct {
		history []VersionHistoryItem
	}

	// VersionHistories contains a set of VersionHistory
	VersionHistories struct {
		versionHistories []VersionHistory
	}
)

// NewVersionHistory initializes new version history
func NewVersionHistory(items []VersionHistoryItem) VersionHistory {
	if len(items) == 0 {
		panic("version history items cannot be empty")
	}

	return VersionHistory{
		history: items,
	}
}

// Update updates the versionHistory slice
func (v *VersionHistory) Update(item VersionHistoryItem) error {
	currentItem := item
	lastItem := &v.history[len(v.history)-1]
	if currentItem.version < lastItem.version {
		return &shared.BadRequestError{
			Message: fmt.Sprintf("cannot update version history with a lower version %v. Last version: %v",
				currentItem.version,
				lastItem.version),
		}
	}

	if currentItem.eventID <= lastItem.eventID {
		return &shared.BadRequestError{
			Message: fmt.Sprintf("cannot add version history with a lower event id %v. Last event id: %v",
				currentItem.eventID,
				lastItem.eventID),
		}
	}

	if currentItem.version > lastItem.version {
		// Add a new history
		v.history = append(v.history, currentItem)
	} else {
		// item.version == lastItem.version && item.eventID > lastItem.eventID
		// Update event  id
		lastItem.eventID = currentItem.eventID
	}
	return nil
}

// FindLowestCommonVersionHistoryItem returns the lowest version history item with the same version
func (v *VersionHistory) FindLowestCommonVersionHistoryItem(remote VersionHistory) (VersionHistoryItem, error) {
	localIdx := len(v.history) - 1
	remoteIdx := len(remote.history) - 1

	for localIdx >= 0 && remoteIdx >= 0 {
		localVersionItem := v.history[localIdx]
		remoteVersionItem := remote.history[remoteIdx]
		if localVersionItem.version == remoteVersionItem.version {
			if localVersionItem.eventID > remoteVersionItem.eventID {
				return remoteVersionItem, nil
			}
			return localVersionItem, nil
		} else if localVersionItem.version > remoteVersionItem.version {
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
	return v.history[len(v.history)-1] == item
}

// NewVersionHistories initialize new version histories
func NewVersionHistories(histories []VersionHistory) VersionHistories {
	if len(histories) == 0 {
		panic("version histories cannot be empty")
	}
	return VersionHistories{
		versionHistories: histories,
	}
}

// FindLowestCommonVersionHistory finds the lowest common version history item among all version histories
func (h *VersionHistories) FindLowestCommonVersionHistory(history VersionHistory) (VersionHistoryItem, VersionHistory, error) {
	var versionHistoryItem VersionHistoryItem
	var versionHistory VersionHistory
	for _, localHistory := range h.versionHistories {
		item, err := localHistory.FindLowestCommonVersionHistoryItem(history)
		if err != nil {
			return versionHistoryItem, versionHistory, err
		}

		if item.eventID > versionHistoryItem.eventID {
			versionHistoryItem = item
			versionHistory = localHistory
		}
	}
	return versionHistoryItem, versionHistory, nil
}

// AddHistory add new history into version histories
// TODO: merge this func with FindLowestCommonVersionHistory
func (h *VersionHistories) AddHistory(item VersionHistoryItem, local VersionHistory, remote VersionHistory) error {
	commonItem := item
	if local.IsAppendable(commonItem) {
		//it won't update h.versionHistories
		for idx, history := range h.versionHistories {
			if reflect.DeepEqual(history, local) {
				h.versionHistories[idx] = remote
			}
		}
	} else {
		h.versionHistories = append(h.versionHistories, remote)
	}
	return nil
}
