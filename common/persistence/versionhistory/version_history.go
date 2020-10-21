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
	"fmt"

	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common"
)

// NewItem create a new version history item
func NewItem(
	inputEventID int64,
	inputVersion int64,
) *historyspb.VersionHistoryItem {

	if inputEventID < 0 || (inputVersion < 0 && inputVersion != common.EmptyVersion) {
		panic(fmt.Sprintf(
			"invalid version history item event ID: %v, version: %v",
			inputEventID,
			inputVersion,
		))
	}

	return &historyspb.VersionHistoryItem{EventId: inputEventID, Version: inputVersion}
}

// DuplicateItem duplicate VersionHistoryItem
func DuplicateItem(item *historyspb.VersionHistoryItem) *historyspb.VersionHistoryItem {
	return NewItem(item.EventId, item.Version)
}

// New create a new version history
func New(
	inputToken []byte,
	inputItems []*historyspb.VersionHistoryItem,
) *historyspb.VersionHistory {

	token := make([]byte, len(inputToken))
	copy(token, inputToken)
	versionHistory := &historyspb.VersionHistory{
		BranchToken: token,
		Items:       nil,
	}

	for _, item := range inputItems {
		if err := AddOrUpdateItem(versionHistory, DuplicateItem(item)); err != nil {
			panic(fmt.Sprintf("unable to initialize version history: %v", err))
		}
	}

	return versionHistory
}

// Duplicate duplicate VersionHistory
func Duplicate(v *historyspb.VersionHistory) *historyspb.VersionHistory {
	return New(v.BranchToken, v.Items)
}

// DuplicateUntilLCAItem duplicate the version history up until LCA item
func DuplicateUntilLCAItem(v *historyspb.VersionHistory,
	lcaItem *historyspb.VersionHistoryItem,
) (*historyspb.VersionHistory, error) {

	versionHistory := New(nil, nil)
	notFoundErr := serviceerror.NewInvalidArgument("version history does not contains the LCA item.")
	for _, item := range v.Items {

		if item.Version < lcaItem.Version {
			if err := AddOrUpdateItem(versionHistory, item); err != nil {
				return nil, err
			}

		} else if item.Version == lcaItem.Version {
			if lcaItem.GetEventId() > item.GetEventId() {
				return nil, notFoundErr
			}
			if err := AddOrUpdateItem(versionHistory, lcaItem); err != nil {
				return nil, err
			}
			return versionHistory, nil

		} else {
			return nil, notFoundErr
		}
	}

	return nil, notFoundErr
}

// SetBranchToken the overwrite the branch token
func SetBranchToken(v *historyspb.VersionHistory, inputToken []byte) error {

	token := make([]byte, len(inputToken))
	copy(token, inputToken)
	v.BranchToken = token
	return nil
}

// GetBranchToken return the branch token
func GetBranchToken(v *historyspb.VersionHistory) []byte {
	token := make([]byte, len(v.BranchToken))
	copy(token, v.BranchToken)
	return token
}

// AddOrUpdateItem updates the versionHistory slice
func AddOrUpdateItem(v *historyspb.VersionHistory, item *historyspb.VersionHistoryItem) error {

	if len(v.Items) == 0 {
		v.Items = []*historyspb.VersionHistoryItem{DuplicateItem(item)}
		return nil
	}

	lastItem := v.Items[len(v.Items)-1]
	if item.Version < lastItem.Version {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("cannot update version history with a lower version %v. Last version: %v", item.Version, lastItem.Version))
	}

	if item.GetEventId() <= lastItem.GetEventId() {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("cannot add version history with a lower event id %v. Last event id: %v", item.GetEventId(), lastItem.GetEventId()))
	}

	if item.Version > lastItem.Version {
		// Add a new history
		v.Items = append(v.Items, DuplicateItem(item))
	} else {
		// item.Version == lastItem.Version && item.EventID > lastItem.EventID
		// Update event ID
		lastItem.EventId = item.GetEventId()
	}
	return nil
}

// ContainsItem check whether given version history item is included
func ContainsItem(
	v *historyspb.VersionHistory,
	item *historyspb.VersionHistoryItem,
) bool {

	prevEventID := common.FirstEventID - 1
	for _, currentItem := range v.Items {
		if item.GetVersion() == currentItem.GetVersion() {
			if prevEventID < item.GetEventId() && item.GetEventId() <= currentItem.GetEventId() {
				return true
			}
		} else if item.GetVersion() < currentItem.GetVersion() {
			return false
		}
		prevEventID = currentItem.GetEventId()
	}
	return false
}

// FindLCAItem returns the lowest common ancestor version history item
func FindLCAItem(
	v *historyspb.VersionHistory,
	remote *historyspb.VersionHistory,
) (*historyspb.VersionHistoryItem, error) {

	localIndex := len(v.Items) - 1
	remoteIndex := len(remote.Items) - 1

	for localIndex >= 0 && remoteIndex >= 0 {
		localVersionItem := v.Items[localIndex]
		remoteVersionItem := remote.Items[remoteIndex]

		if localVersionItem.Version == remoteVersionItem.Version {
			if localVersionItem.GetEventId() > remoteVersionItem.GetEventId() {
				return DuplicateItem(remoteVersionItem), nil
			}
			return DuplicateItem(localVersionItem), nil
		} else if localVersionItem.Version > remoteVersionItem.Version {
			localIndex--
		} else {
			// localVersionItem.Version < remoteVersionItem.Version
			remoteIndex--
		}
	}

	return nil, serviceerror.NewInvalidArgument("version history is malformed. No joint point found.")
}

// IsLCAAppendable checks if a LCA version history item is appendable
func IsLCAAppendable(
	v *historyspb.VersionHistory,
	item *historyspb.VersionHistoryItem,
) bool {

	if len(v.Items) == 0 {
		panic("version history not initialized")
	}
	if item == nil {
		panic("version history item is null")
	}

	return *v.Items[len(v.Items)-1] == *item
}

// GetFirstItem return the first version history item
func GetFirstItem(v *historyspb.VersionHistory) (*historyspb.VersionHistoryItem, error) {

	if len(v.Items) == 0 {
		return nil, serviceerror.NewInvalidArgument("version history is empty.")
	}

	return DuplicateItem(v.Items[0]), nil
}

// GetLastItem return the last version history item
func GetLastItem(v *historyspb.VersionHistory) (*historyspb.VersionHistoryItem, error) {

	if len(v.Items) == 0 {
		return nil, serviceerror.NewInvalidArgument("version history is empty.")
	}

	return DuplicateItem(v.Items[len(v.Items)-1]), nil
}

// GetEventVersion return the corresponding event version of an event ID
func GetEventVersion(
	v *historyspb.VersionHistory,
	eventID int64,
) (int64, error) {

	lastItem, err := GetLastItem(v)
	if err != nil {
		return 0, err
	}
	if eventID < common.FirstEventID || eventID > lastItem.GetEventId() {
		return 0, serviceerror.NewInvalidArgument("input event ID is not in range.")
	}

	// items are sorted by eventID & version
	// so the fist item with item event ID >= input event ID
	// the item version is the result
	for _, currentItem := range v.Items {
		if eventID <= currentItem.GetEventId() {
			return currentItem.GetVersion(), nil
		}
	}
	return 0, serviceerror.NewInvalidArgument("input event ID is not in range.")
}

// IsEmpty indicate whether version history is empty
func IsEmpty(v *historyspb.VersionHistory) bool {
	return len(v.Items) == 0
}

// NewVHS create a new version histories
func NewVHS(
	versionHistory *historyspb.VersionHistory,
) *historyspb.VersionHistories {

	if versionHistory == nil {
		panic("version history cannot be null")
	}

	return &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories:                  []*historyspb.VersionHistory{versionHistory},
	}
}

// DuplicateVHS duplicate VersionHistories
func DuplicateVHS(h *historyspb.VersionHistories) *historyspb.VersionHistories {

	currentVersionHistoryIndex := h.CurrentVersionHistoryIndex
	var histories []*historyspb.VersionHistory
	for _, history := range h.Histories {
		histories = append(histories, Duplicate(history))
	}

	return &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: currentVersionHistoryIndex,
		Histories:                  histories,
	}
}

// GetVersionHistory get the version history according to index provided
func GetVersionHistory(
	h *historyspb.VersionHistories,
	branchIndex int32,
) (*historyspb.VersionHistory, error) {

	if branchIndex < 0 || branchIndex > int32(len(h.Histories)) {
		return nil, serviceerror.NewInvalidArgument("invalid branch index.")
	}

	return h.Histories[branchIndex], nil
}

// AddVersionHistory add a version history and return the whether current branch is changed
func AddVersionHistory(
	h *historyspb.VersionHistories,
	v *historyspb.VersionHistory,
) (bool, int32, error) {

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

	newVersionHistory := Duplicate(v)
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

// FindLCAVersionHistoryIndexAndItem finds the lowest common ancestor version history index
// along with corresponding item
func FindLCAVersionHistoryIndexAndItem(
	h *historyspb.VersionHistories,
	incomingHistory *historyspb.VersionHistory,
) (int32, *historyspb.VersionHistoryItem, error) {

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

// FindFirstVersionHistoryIndexByItem find the first version history index which
// contains the given version history item
func FindFirstVersionHistoryIndexByItem(
	h *historyspb.VersionHistories,
	item *historyspb.VersionHistoryItem,
) (int32, error) {

	for index, localHistory := range h.Histories {
		if ContainsItem(localHistory, item) {
			return int32(index), nil
		}
	}
	return 0, serviceerror.NewInvalidArgument("version histories does not contains given item.")
}

// IsRebuilt returns true if the current branch index's last write version is not the largest
// among all branches' last write version
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

// SetCurrentVersionHistoryIndex set the current branch index
func SetCurrentVersionHistoryIndex(
	h *historyspb.VersionHistories,
	index int32,
) error {

	if index < 0 || index >= int32(len(h.Histories)) {
		return serviceerror.NewInvalidArgument("invalid current branch index.")
	}

	h.CurrentVersionHistoryIndex = index
	return nil
}

// GetCurrentVersionHistory get the current version history
func GetCurrentVersionHistory(h *historyspb.VersionHistories) (*historyspb.VersionHistory, error) {

	return GetVersionHistory(h, h.GetCurrentVersionHistoryIndex())
}
