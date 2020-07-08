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

package persistencetests

import (
	"testing"

	"go.temporal.io/server/common/persistence"
)

func TestGarbageCleanupInfo(t *testing.T) {
	namespaceID := "10000000-5000-f000-f000-000000000000"
	workflowID := "workflow-id"
	runID := "10000000-5000-f000-f000-000000000002"

	info := persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, runID)
	namespaceID2, workflowID2, runID2, err := persistence.SplitHistoryGarbageCleanupInfo(info)
	if err != nil || namespaceID != namespaceID2 || workflowID != workflowID2 || runID != runID2 {
		t.Fail()
	}
}

func TestGarbageCleanupInfo_WithColonInWorklfowID(t *testing.T) {
	namespaceID := "10000000-5000-f000-f000-000000000000"
	workflowID := "workflow-id:2"
	runID := "10000000-5000-f000-f000-000000000002"

	info := persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, runID)
	namespaceID2, workflowID2, runID2, err := persistence.SplitHistoryGarbageCleanupInfo(info)
	if err != nil || namespaceID != namespaceID2 || workflowID != workflowID2 || runID != runID2 {
		t.Fail()
	}
}
