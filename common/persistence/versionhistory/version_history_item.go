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

	historyspb "go.temporal.io/server/api/history/v1"
)

// NewVersionHistoryItem create a new instance of VersionHistoryItem.
func NewVersionHistoryItem(eventID int64, version int64) *historyspb.VersionHistoryItem {
	if eventID < 0 || version < 0 {
		panic(fmt.Sprintf("invalid version history item event ID: %v, version: %v", eventID, version))
	}

	return &historyspb.VersionHistoryItem{EventId: eventID, Version: version}
}

// CopyVersionHistoryItem create a new instance of VersionHistoryItem.
func CopyVersionHistoryItem(item *historyspb.VersionHistoryItem) *historyspb.VersionHistoryItem {
	return NewVersionHistoryItem(item.EventId, item.Version)
}

// IsEqualVersionHistoryItem checks whether version history items are equal
func IsEqualVersionHistoryItem(item1 *historyspb.VersionHistoryItem, item2 *historyspb.VersionHistoryItem) bool {
	return item1.EventId == item2.EventId && item1.Version == item2.Version
}
