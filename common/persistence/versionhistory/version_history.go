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

// New create a new instance of VersionHistory.
func New(branchToken []byte, items []*historyspb.VersionHistoryItem) *historyspb.VersionHistory {
	return &historyspb.VersionHistory{
		BranchToken: branchToken,
		Items:       items,
	}
}

// Copy VersionHistory.
func Copy(v *historyspb.VersionHistory) *historyspb.VersionHistory {
	token := make([]byte, len(v.BranchToken))
	copy(token, v.BranchToken)

	var items []*historyspb.VersionHistoryItem
	for _, item := range v.Items {
		itemCopy := *item
		items = append(items, &itemCopy)
	}

	return New(token, items)
}

// CopyUntilLCAItem returns copy of VersionHistory up until LCA item.
func CopyUntilLCAItem(v *historyspb.VersionHistory, lcaItem *historyspb.VersionHistoryItem) (*historyspb.VersionHistory, error) {
	versionHistory := &historyspb.VersionHistory{}
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

// SetBranchToken sets the branch token.
func SetBranchToken(v *historyspb.VersionHistory, branchToken []byte) {
	v.BranchToken = make([]byte, len(branchToken))
	copy(v.BranchToken, branchToken)
}

// AddOrUpdateItem updates the VersionHistory with new VersionHistoryItem.
func AddOrUpdateItem(v *historyspb.VersionHistory, item *historyspb.VersionHistoryItem) error {
	if len(v.Items) == 0 {
		itemCopy := *item
		v.Items = []*historyspb.VersionHistoryItem{&itemCopy}
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
		itemCopy := *item
		v.Items = append(v.Items, &itemCopy)
	} else {
		// item.Version == lastItem.Version && item.EventID > lastItem.EventID
		// Update event ID
		lastItem.EventId = item.GetEventId()
	}
	return nil
}

// ContainsItem check whether VersionHistory has given VersionHistoryItem.
func ContainsItem(v *historyspb.VersionHistory, item *historyspb.VersionHistoryItem) bool {
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

// FindLCAItem returns the lowest common ancestor VersionHistoryItem.
func FindLCAItem(v *historyspb.VersionHistory, remote *historyspb.VersionHistory) (*historyspb.VersionHistoryItem, error) {
	localIndex := len(v.Items) - 1
	remoteIndex := len(remote.Items) - 1

	for localIndex >= 0 && remoteIndex >= 0 {
		localVersionItem := v.Items[localIndex]
		remoteVersionItem := remote.Items[remoteIndex]

		if localVersionItem.Version == remoteVersionItem.Version {
			if localVersionItem.GetEventId() > remoteVersionItem.GetEventId() {
				return remoteVersionItem, nil
			}
			return localVersionItem, nil
		} else if localVersionItem.Version > remoteVersionItem.Version {
			localIndex--
		} else {
			// localVersionItem.Version < remoteVersionItem.Version
			remoteIndex--
		}
	}

	return nil, serviceerror.NewInvalidArgument("version history is malformed. No joint point found.")
}

// IsLCAAppendable checks if a LCA VersionHistoryItem is appendable.
func IsLCAAppendable(v *historyspb.VersionHistory, item *historyspb.VersionHistoryItem) bool {
	if len(v.Items) == 0 {
		panic("version history not initialized")
	}
	if item == nil {
		panic("item is nil")
	}

	return *v.Items[len(v.Items)-1] == *item
}

// GetFirstItem return the first VersionHistoryItem.
func GetFirstItem(v *historyspb.VersionHistory) (*historyspb.VersionHistoryItem, error) {
	if len(v.Items) == 0 {
		return nil, serviceerror.NewInvalidArgument("version history is empty.")
	}
	return v.Items[0], nil
}

// GetLastItem return the last VersionHistoryItem.
func GetLastItem(v *historyspb.VersionHistory) (*historyspb.VersionHistoryItem, error) {
	if len(v.Items) == 0 {
		return nil, serviceerror.NewInvalidArgument("version history is empty.")
	}
	return v.Items[len(v.Items)-1], nil
}

// GetEventVersion return the corresponding event version of an event ID.
func GetEventVersion(v *historyspb.VersionHistory, eventID int64) (int64, error) {
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
