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
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
)

// NewVersionHistories create a new instance of VersionHistories.
func NewVersionHistories(versionHistory *historyspb.VersionHistory) *historyspb.VersionHistories {
	if versionHistory == nil {
		panic("version history cannot be null")
	}

	return &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories:                  []*historyspb.VersionHistory{versionHistory},
	}
}

// Copy VersionHistories.
func CopyVersionHistories(h *historyspb.VersionHistories) *historyspb.VersionHistories {
	var histories []*historyspb.VersionHistory
	for _, history := range h.Histories {
		histories = append(histories, CopyVersionHistory(history))
	}

	return &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: h.CurrentVersionHistoryIndex,
		Histories:                  histories,
	}
}

// GetVersionHistory gets the VersionHistory according to index provided.
func GetVersionHistory(h *historyspb.VersionHistories, index int32) (*historyspb.VersionHistory, error) {
	if index < 0 || index >= int32(len(h.Histories)) {
		return nil, serviceerror.NewInvalidArgument("invalid index.")
	}

	return h.Histories[index], nil
}

// AddVersionHistory adds a VersionHistory and return the whether current branch is changed.
func AddVersionHistory(h *historyspb.VersionHistories, v *historyspb.VersionHistory) (bool, int32, error) {
	if v == nil {
		return false, 0, serviceerror.NewInvalidArgument("version histories is null.")
	}

	// assuming existing version histories inside are valid
	incomingFirstItem, err := GetFirstItem(v)
	if err != nil {
		return false, 0, err
	}

	currentVersionHistory, err := GetVersionHistory(h, h.CurrentVersionHistoryIndex)
	if err != nil {
		return false, 0, err
	}
	currentFirstItem, err := GetFirstItem(currentVersionHistory)
	if err != nil {
		return false, 0, err
	}

	if incomingFirstItem.Version != currentFirstItem.Version {
		return false, 0, serviceerror.NewInvalidArgument("version history first item does not match.")
	}

	// TODO maybe we need more strict validation

	newVersionHistory := CopyVersionHistory(v)
	h.Histories = append(h.Histories, newVersionHistory)
	newVersionHistoryIndex := int32(len(h.Histories)) - 1

	// check if need to switch current branch
	newLastItem, err := GetLastItem(newVersionHistory)
	if err != nil {
		return false, 0, err
	}
	currentLastItem, err := GetLastItem(currentVersionHistory)
	if err != nil {
		return false, 0, err
	}

	currentBranchChanged := false
	if newLastItem.Version > currentLastItem.Version {
		currentBranchChanged = true
		h.CurrentVersionHistoryIndex = newVersionHistoryIndex
	}
	return currentBranchChanged, newVersionHistoryIndex, nil
}

// FindLCAIndexAndItem finds the lowest common ancestor VersionHistory index and corresponding item.
func FindLCAIndexAndItem(h *historyspb.VersionHistories, incomingHistory *historyspb.VersionHistory) (int32, *historyspb.VersionHistoryItem, error) {
	var versionHistoryIndex int32
	var versionHistoryLength int32
	var versionHistoryItem *historyspb.VersionHistoryItem

	for index, localHistory := range h.Histories {
		item, err := FindLCAItem(localHistory, incomingHistory)
		if err != nil {
			return 0, nil, err
		}

		// if not set
		if versionHistoryItem == nil ||
			// if seeing LCA item with higher event ID
			item.GetEventId() > versionHistoryItem.GetEventId() ||
			// if seeing LCA item with equal event ID but shorter history
			(item.GetEventId() == versionHistoryItem.GetEventId() && int32(len(localHistory.Items)) < versionHistoryLength) {

			versionHistoryIndex = int32(index)
			versionHistoryLength = int32(len(localHistory.Items))
			versionHistoryItem = item
		}
	}
	return versionHistoryIndex, versionHistoryItem, nil
}

// FindItemFirstIndex find the first VersionHistory index which contains the given version history item.
func FindItemFirstIndex(h *historyspb.VersionHistories, item *historyspb.VersionHistoryItem) (int32, error) {
	for index, history := range h.Histories {
		if ContainsItem(history, item) {
			return int32(index), nil
		}
	}
	return 0, serviceerror.NewInvalidArgument("version histories does not contains given item.")
}

// IsRebuilt returns true if the current branch index's last write version is not the largest among all branches' last write version.
func IsRebuilt(h *historyspb.VersionHistories) (bool, error) {
	currentVersionHistory, err := GetCurrentVersionHistory(h)
	if err != nil {
		return false, err
	}

	currentLastItem, err := GetLastItem(currentVersionHistory)
	if err != nil {
		return false, err
	}

	for _, versionHistory := range h.Histories {
		lastItem, err := GetLastItem(versionHistory)
		if err != nil {
			return false, err
		}
		if lastItem.GetVersion() > currentLastItem.GetVersion() {
			return true, nil
		}
	}

	return false, nil
}

// SetCurrentIndex set the current VersionHistory index.
func SetCurrentIndex(h *historyspb.VersionHistories, currentVersionHistoryIndex int32) error {
	if currentVersionHistoryIndex < 0 || currentVersionHistoryIndex >= int32(len(h.Histories)) {
		return serviceerror.NewInvalidArgument("invalid current version history index.")
	}

	h.CurrentVersionHistoryIndex = currentVersionHistoryIndex
	return nil
}

// GetCurrentVersionHistory gets the current VersionHistory.
func GetCurrentVersionHistory(h *historyspb.VersionHistories) (*historyspb.VersionHistory, error) {
	return GetVersionHistory(h, h.GetCurrentVersionHistoryIndex())
}
