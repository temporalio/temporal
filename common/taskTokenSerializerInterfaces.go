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

package common

type (
	// TaskTokenSerializer serializes task tokens
	TaskTokenSerializer interface {
		Serialize(token *TaskToken) ([]byte, error)
		Deserialize(data []byte) (*TaskToken, error)
		SerializeQueryTaskToken(token *QueryTaskToken) ([]byte, error)
		DeserializeQueryTaskToken(data []byte) (*QueryTaskToken, error)
	}

	// TaskToken identifies a task
	TaskToken struct {
		DomainID        []byte `json:"domainId"`
		WorkflowID      string `json:"workflowId"`
		RunID           []byte `json:"runId"`
		ScheduleID      int64  `json:"scheduleId"`
		ScheduleAttempt int64  `json:"scheduleAttempt"`
		ActivityID      string `json:"activityId"`
	}

	// QueryTaskToken identifies a query task
	QueryTaskToken struct {
		DomainID string `json:"domainId"`
		TaskList string `json:"taskList"`
		TaskID   string `json:"taskId"`
	}
)
