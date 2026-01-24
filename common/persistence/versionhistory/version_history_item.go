package versionhistory

import (
	"fmt"

	historyspb "go.temporal.io/server/api/history/v1"
)

// NewVersionHistoryItem create a new instance of VersionHistoryItem.
func NewVersionHistoryItem(eventID int64, version int64) *historyspb.VersionHistoryItem {
	if eventID < 0 || version < 0 {
		panic(fmt.Sprintf("invalid version history item event ID: %v, version: %v", eventID, version))
	}

	return historyspb.VersionHistoryItem_builder{EventId: eventID, Version: version}.Build()
}

// CopyVersionHistoryItem create a new instance of VersionHistoryItem.
func CopyVersionHistoryItem(item *historyspb.VersionHistoryItem) *historyspb.VersionHistoryItem {
	return NewVersionHistoryItem(item.GetEventId(), item.GetVersion())
}

// IsEqualVersionHistoryItem checks whether version history items are equal
func IsEqualVersionHistoryItem(item1 *historyspb.VersionHistoryItem, item2 *historyspb.VersionHistoryItem) bool {
	return item1.GetEventId() == item2.GetEventId() && item1.GetVersion() == item2.GetVersion()
}

// IsEqualVersionHistoryItems checks whether version history items are equal
func IsEqualVersionHistoryItems(items1 []*historyspb.VersionHistoryItem, items2 []*historyspb.VersionHistoryItem) bool {
	if len(items1) != len(items2) {
		return false
	}
	for i := 0; i < len(items1); i++ {
		if !IsEqualVersionHistoryItem(items1[i], items2[i]) {
			return false
		}
	}
	return true
}

// CompareVersionHistoryItem compares 2 version history items
func CompareVersionHistoryItem(item1 *historyspb.VersionHistoryItem, item2 *historyspb.VersionHistoryItem) int {
	if item1.GetVersion() < item2.GetVersion() {
		return -1
	}
	if item1.GetVersion() > item2.GetVersion() {
		return 1
	}

	// item1.Version == item2.Version
	if item1.GetEventId() < item2.GetEventId() {
		return -1
	}
	if item1.GetEventId() > item2.GetEventId() {
		return 1
	}
	return 0
}
