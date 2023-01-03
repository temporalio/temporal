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

package tasks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

func TestArchiveExecutionTask(t *testing.T) {
	workflowKey := definition.NewWorkflowKey("namespace", "workflowID", "runID")
	visibilityTimestamp := time.Now()
	taskID := int64(123)
	version := int64(456)
	task := &ArchiveExecutionTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: visibilityTimestamp,
		TaskID:              taskID,
		Version:             version,
	}
	assert.Equal(t, NewKey(visibilityTimestamp, taskID), task.GetKey())
	assert.Equal(t, taskID, task.GetTaskID())
	assert.Equal(t, visibilityTimestamp, task.GetVisibilityTime())
	assert.Equal(t, version, task.GetVersion())
	assert.Equal(t, CategoryArchival, task.GetCategory())
	assert.Equal(t, enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION, task.GetType())
}
