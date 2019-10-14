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
	"bytes"
	"fmt"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

// NewVersionHistoryItem create a new version history item
func NewVersionHistoryItem(
	inputEventID int64,
	inputVersion int64,
) *VersionHistoryItem {

	if inputEventID < 0 || (inputVersion < 0 && inputVersion != common.EmptyVersion) {
		panic(fmt.Sprintf(
			"invalid version history item event ID: %v, version: %v",
			inputEventID,
			inputVersion,
		))
	}

	return &VersionHistoryItem{EventID: inputEventID, Version: inputVersion}
}

// NewVersionHistoryItemFromThrift create a new version history item from thrift object
func NewVersionHistoryItemFromThrift(
	input *shared.VersionHistoryItem,
) *VersionHistoryItem {

	if input == nil {
		panic("version history item is null")
	}

	return NewVersionHistoryItem(input.GetEventID(), input.GetVersion())
}

// Duplicate duplicate VersionHistoryItem
func (item *VersionHistoryItem) Duplicate() *VersionHistoryItem {

	return NewVersionHistoryItem(item.EventID, item.Version)
}

// ToThrift return thrift format of version history item
func (item *VersionHistoryItem) ToThrift() *shared.VersionHistoryItem {

	return &shared.VersionHistoryItem{
		EventID: common.Int64Ptr(item.EventID),
		Version: common.Int64Ptr(item.Version),
	}
}

// GetEventID return the event ID
func (item *VersionHistoryItem) GetEventID() int64 {
	return item.EventID
}

// GetVersion return the event ID
func (item *VersionHistoryItem) GetVersion() int64 {
	return item.Version
}

// Equals test if this version history itme and input version history item  are the same
func (item *VersionHistoryItem) Equals(input *VersionHistoryItem) bool {
	return item.Version == input.Version && item.EventID == input.EventID
}

// NewVersionHistory create a new version history
func NewVersionHistory(
	inputToken []byte,
	inputItems []*VersionHistoryItem,
) *VersionHistory {

	token := make([]byte, len(inputToken))
	copy(token, inputToken)
	versionHistory := &VersionHistory{
		BranchToken: token,
		Items:       nil,
	}

	for _, item := range inputItems {
		if err := versionHistory.AddOrUpdateItem(item.Duplicate()); err != nil {
			panic(fmt.Sprintf("unable to initialize version history: %v", err))
		}
	}

	return versionHistory
}

// NewVersionHistoryFromThrift create a new version history from thrift object
func NewVersionHistoryFromThrift(
	input *shared.VersionHistory,
) *VersionHistory {

	if input == nil {
		panic("version history is null")
	}

	items := []*VersionHistoryItem{}
	for _, item := range input.Items {
		items = append(items, NewVersionHistoryItemFromThrift(item))
	}
	return NewVersionHistory(input.BranchToken, items)
}

// Duplicate duplicate VersionHistory
func (v *VersionHistory) Duplicate() *VersionHistory {

	return NewVersionHistory(v.BranchToken, v.Items)
}

// ToThrift return thrift format of version history
func (v *VersionHistory) ToThrift() *shared.VersionHistory {

	token := make([]byte, len(v.BranchToken))
	copy(token, v.BranchToken)
	items := []*shared.VersionHistoryItem{}
	for _, item := range v.Items {
		items = append(items, item.ToThrift())
	}

	tHistory := &shared.VersionHistory{
		BranchToken: token,
		Items:       items,
	}
	return tHistory
}

// DuplicateUntilLCAItem duplicate the version history up until LCA item
func (v *VersionHistory) DuplicateUntilLCAItem(
	lcaItem *VersionHistoryItem,
) (*VersionHistory, error) {

	versionHistory := NewVersionHistory(nil, nil)
	notFoundErr := &shared.BadRequestError{
		Message: "version history does not contains the LCA item.",
	}
	for _, item := range v.Items {

		if item.Version < lcaItem.Version {
			if err := versionHistory.AddOrUpdateItem(item); err != nil {
				return nil, err
			}

		} else if item.Version == lcaItem.Version {
			if lcaItem.EventID > item.EventID {
				return nil, notFoundErr
			}
			if err := versionHistory.AddOrUpdateItem(lcaItem); err != nil {
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
func (v *VersionHistory) SetBranchToken(
	inputToken []byte,
) error {

	token := make([]byte, len(inputToken))
	copy(token, inputToken)
	v.BranchToken = token
	return nil
}

// GetBranchToken return the branch token
func (v *VersionHistory) GetBranchToken() []byte {
	token := make([]byte, len(v.BranchToken))
	copy(token, v.BranchToken)
	return token
}

// AddOrUpdateItem updates the versionHistory slice
func (v *VersionHistory) AddOrUpdateItem(
	item *VersionHistoryItem,
) error {

	if len(v.Items) == 0 {
		v.Items = []*VersionHistoryItem{item.Duplicate()}
		return nil
	}

	lastItem := v.Items[len(v.Items)-1]
	if item.Version < lastItem.Version {
		return &shared.BadRequestError{Message: fmt.Sprintf(
			"cannot update version history with a lower version %v. Last version: %v",
			item.Version, lastItem.Version,
		)}
	}

	if item.EventID <= lastItem.EventID {
		return &shared.BadRequestError{Message: fmt.Sprintf(
			"cannot add version history with a lower event id %v. Last event id: %v",
			item.EventID, lastItem.EventID,
		)}
	}

	if item.Version > lastItem.Version {
		// Add a new history
		v.Items = append(v.Items, item.Duplicate())
	} else {
		// item.Version == lastItem.Version && item.EventID > lastItem.EventID
		// Update event ID
		lastItem.EventID = item.EventID
	}
	return nil
}

// ContainsItem check whether given version history item is included
func (v *VersionHistory) ContainsItem(
	item *VersionHistoryItem,
) bool {

	prevEventID := common.FirstEventID - 1
	for _, currentItem := range v.Items {
		if item.GetVersion() == currentItem.GetVersion() {
			// this is a special handling for event id = 0
			if (item.GetEventID() == common.FirstEventID-1) && item.GetEventID() <= currentItem.GetEventID() {
				return true
			}
			if prevEventID < item.GetEventID() && item.GetEventID() <= currentItem.GetEventID() {
				return true
			}
		} else if item.GetVersion() < currentItem.GetVersion() {
			return false
		}
		prevEventID = currentItem.GetEventID()
	}
	return false
}

// FindLCAItem returns the lowest common ancestor version history item
func (v *VersionHistory) FindLCAItem(
	remote *VersionHistory,
) (*VersionHistoryItem, error) {

	localIndex := len(v.Items) - 1
	remoteIndex := len(remote.Items) - 1

	for localIndex >= 0 && remoteIndex >= 0 {
		localVersionItem := v.Items[localIndex]
		remoteVersionItem := remote.Items[remoteIndex]

		if localVersionItem.Version == remoteVersionItem.Version {
			if localVersionItem.EventID > remoteVersionItem.EventID {
				return remoteVersionItem.Duplicate(), nil
			}
			return localVersionItem.Duplicate(), nil
		} else if localVersionItem.Version > remoteVersionItem.Version {
			localIndex--
		} else {
			// localVersionItem.Version < remoteVersionItem.Version
			remoteIndex--
		}
	}

	return nil, &shared.BadRequestError{
		Message: "version history is malformed. No joint point found.",
	}
}

// IsLCAAppendable checks if a LCA version history item is appendable
func (v *VersionHistory) IsLCAAppendable(
	item *VersionHistoryItem,
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
func (v *VersionHistory) GetFirstItem() (*VersionHistoryItem, error) {

	if len(v.Items) == 0 {
		return nil, &shared.BadRequestError{Message: "version history is empty."}
	}

	return v.Items[0].Duplicate(), nil
}

// GetLastItem return the last version history item
func (v *VersionHistory) GetLastItem() (*VersionHistoryItem, error) {

	if len(v.Items) == 0 {
		return nil, &shared.BadRequestError{Message: "version history is empty."}
	}

	return v.Items[len(v.Items)-1].Duplicate(), nil
}

// GetEventVersion return the corresponding event version of an event ID
func (v *VersionHistory) GetEventVersion(
	eventID int64,
) (int64, error) {

	lastItem, err := v.GetLastItem()
	if err != nil {
		return 0, err
	}
	if eventID < common.FirstEventID || eventID > lastItem.GetEventID() {
		return 0, &shared.BadRequestError{Message: "input event ID is not in range."}
	}

	// items are sorted by eventID & version
	// so the fist item with item event ID >= input event ID
	// the item version is the result
	for _, currentItem := range v.Items {
		if eventID <= currentItem.GetEventID() {
			return currentItem.GetVersion(), nil
		}
	}
	return 0, &shared.BadRequestError{Message: "input event ID is not in range."}
}

// IsEmpty indicate whether version history is empty
func (v *VersionHistory) IsEmpty() bool {
	return len(v.Items) == 0
}

// Equals test if this version history and input version history are the same
func (v *VersionHistory) Equals(
	input *VersionHistory,
) bool {

	if !bytes.Equal(v.BranchToken, input.BranchToken) {
		return false
	}

	if len(v.Items) != len(input.Items) {
		return false
	}

	for index, localItem := range v.Items {
		incomingItem := input.Items[index]
		if !localItem.Equals(incomingItem) {
			return false
		}
	}
	return true
}

// NewVersionHistories create a new version histories
func NewVersionHistories(
	versionHistory *VersionHistory,
) *VersionHistories {

	if versionHistory == nil {
		panic("version history cannot be null")
	}

	return &VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories:                  []*VersionHistory{versionHistory},
	}
}

// NewVersionHistoriesFromThrift create a new version histories from thrift object
func NewVersionHistoriesFromThrift(
	input *shared.VersionHistories,
) *VersionHistories {

	if input == nil {
		panic("version histories is null")
	}
	if len(input.Histories) == 0 {
		panic("version histories cannot have empty")
	}

	currentVersionHistoryIndex := int(input.GetCurrentVersionHistoryIndex())

	versionHistories := NewVersionHistories(NewVersionHistoryFromThrift(input.Histories[0]))
	for i := 1; i < len(input.Histories); i++ {
		_, _, err := versionHistories.AddVersionHistory(NewVersionHistoryFromThrift(input.Histories[i]))
		if err != nil {
			panic(fmt.Sprintf("unable to initialize version histories: %v", err))
		}
	}

	if currentVersionHistoryIndex != versionHistories.CurrentVersionHistoryIndex {
		panic("unable to initialize version histories: current index mismatch")
	}

	return versionHistories
}

// Duplicate duplicate VersionHistories
func (h *VersionHistories) Duplicate() *VersionHistories {

	currentVersionHistoryIndex := h.CurrentVersionHistoryIndex
	histories := []*VersionHistory{}
	for _, history := range h.Histories {
		histories = append(histories, history.Duplicate())
	}

	return &VersionHistories{
		CurrentVersionHistoryIndex: currentVersionHistoryIndex,
		Histories:                  histories,
	}
}

// ToThrift return thrift format of version histories
func (h *VersionHistories) ToThrift() *shared.VersionHistories {

	currentVersionHistoryIndex := h.CurrentVersionHistoryIndex
	histories := []*shared.VersionHistory{}
	for _, history := range h.Histories {
		histories = append(histories, history.ToThrift())
	}

	return &shared.VersionHistories{
		CurrentVersionHistoryIndex: common.Int32Ptr(int32(currentVersionHistoryIndex)),
		Histories:                  histories,
	}
}

// GetVersionHistory get the version history according to index provided
func (h *VersionHistories) GetVersionHistory(
	branchIndex int,
) (*VersionHistory, error) {

	if branchIndex < 0 || branchIndex > len(h.Histories) {
		return nil, &shared.BadRequestError{Message: "invalid branch index."}
	}

	return h.Histories[branchIndex], nil
}

// AddVersionHistory add a version history and return the whether current branch is changed
func (h *VersionHistories) AddVersionHistory(
	v *VersionHistory,
) (bool, int, error) {

	if v == nil {
		return false, 0, &shared.BadRequestError{Message: "version histories is null."}
	}

	// assuming existing version histories inside are valid
	incomingFirstItem, err := v.GetFirstItem()
	if err != nil {
		return false, 0, err
	}

	currentVersionHistory, err := h.GetVersionHistory(h.CurrentVersionHistoryIndex)
	if err != nil {
		return false, 0, err
	}
	currentFirstItem, err := currentVersionHistory.GetFirstItem()
	if err != nil {
		return false, 0, err
	}

	if incomingFirstItem.Version != currentFirstItem.Version {
		return false, 0, &shared.BadRequestError{Message: "version history first item does not match."}
	}

	// TODO maybe we need more strict validation

	newVersionHistory := v.Duplicate()
	h.Histories = append(h.Histories, newVersionHistory)
	newVersionHistoryIndex := len(h.Histories) - 1

	// check if need to switch current branch
	newLastItem, err := newVersionHistory.GetLastItem()
	if err != nil {
		return false, 0, err
	}
	currentLastItem, err := currentVersionHistory.GetLastItem()
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
func (h *VersionHistories) FindLCAVersionHistoryIndexAndItem(
	incomingHistory *VersionHistory,
) (int, *VersionHistoryItem, error) {

	var versionHistoryIndex int
	var versionHistoryLength int
	var versionHistoryItem *VersionHistoryItem

	for index, localHistory := range h.Histories {
		item, err := localHistory.FindLCAItem(incomingHistory)
		if err != nil {
			return 0, nil, err
		}

		// if not set
		if versionHistoryItem == nil ||
			// if seeing LCA item with higher event ID
			item.EventID > versionHistoryItem.EventID ||
			// if seeing LCA item with equal event ID but shorter history
			(item.EventID == versionHistoryItem.EventID && len(localHistory.Items) < versionHistoryLength) {

			versionHistoryIndex = index
			versionHistoryLength = len(localHistory.Items)
			versionHistoryItem = item
		}
	}
	return versionHistoryIndex, versionHistoryItem, nil
}

// FindFirstVersionHistoryIndexByItem find the first version history index which
// contains the given version history item
func (h *VersionHistories) FindFirstVersionHistoryIndexByItem(
	item *VersionHistoryItem,
) (int, error) {

	for index, localHistory := range h.Histories {
		if localHistory.ContainsItem(item) {
			return index, nil
		}
	}
	return 0, &shared.BadRequestError{Message: "version histories does not contains given item."}
}

// IsRebuilt returns true if the current branch index's last write version is not the largest
// among all branches' last write version
func (h *VersionHistories) IsRebuilt() (bool, error) {

	currentVersionHistory, err := h.GetCurrentVersionHistory()
	if err != nil {
		return false, err
	}

	currentLastItem, err := currentVersionHistory.GetLastItem()
	if err != nil {
		return false, err
	}

	for _, versionHistory := range h.Histories {
		lastItem, err := versionHistory.GetLastItem()
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
func (h *VersionHistories) SetCurrentVersionHistoryIndex(
	index int,
) error {

	if index < 0 || index >= len(h.Histories) {
		return &shared.BadRequestError{Message: "invalid current branch index."}
	}

	h.CurrentVersionHistoryIndex = index
	return nil
}

// GetCurrentVersionHistoryIndex get the current branch index
func (h *VersionHistories) GetCurrentVersionHistoryIndex() int {
	return h.CurrentVersionHistoryIndex
}

// GetCurrentVersionHistory get the current version history
func (h *VersionHistories) GetCurrentVersionHistory() (*VersionHistory, error) {

	return h.GetVersionHistory(h.GetCurrentVersionHistoryIndex())
}
