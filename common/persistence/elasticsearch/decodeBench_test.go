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

package elasticsearch

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/definition"
	p "go.temporal.io/server/common/persistence"
)

var (
	data = []byte(`{"ExecutionStatus": 1,
         "CloseTime": 1547596872817380000,
         "NamespaceId": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
         "HistoryLength": 29,
         "KafkaKey": "7-619",
         "RunId": "e481009e-14b3-45ae-91af-dce6e2a88365",
         "StartTime": 1547596872371000000,
         "WorkflowId": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
         "WorkflowType": "TestWorkflowExecute",
 		 "Encoding" : "proto3",
		 "TaskQueue" : "taskQueue", 
		 "Memo" : "deadbeef====="}`)
)

/*
BenchmarkJSONDecodeToType-8       200000              9321 ns/op
BenchmarkJSONDecodeToMap-8        100000             12878 ns/op
*/

func BenchmarkJSONDecodeToType(b *testing.B) {
	bytes := (*json.RawMessage)(&data)
	for i := 0; i < b.N; i++ {
		var source *visibilityRecord
		json.Unmarshal(*bytes, &source)
		record := &p.VisibilityWorkflowExecutionInfo{
			WorkflowID:    source.WorkflowID,
			RunID:         source.RunID,
			TypeName:      source.WorkflowType,
			StartTime:     time.Unix(0, source.StartTime),
			ExecutionTime: time.Unix(0, source.ExecutionTime),
			Memo:          p.NewDataBlob(source.Memo, source.Encoding),
			TaskQueue:     source.TaskQueue,
			CloseTime:     time.Unix(0, source.CloseTime),
			Status:        source.ExecutionStatus,
			HistoryLength: source.HistoryLength,
		}
		_ = record
	}
}

func BenchmarkJSONDecodeToMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var source map[string]interface{}
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber()
		d.Decode(&source)

		startTime, _ := source[definition.StartTime].(json.Number).Int64()
		executionTime, _ := source[definition.StartTime].(json.Number).Int64()
		closeTime, _ := source[definition.CloseTime].(json.Number).Int64()
		status, _ := source[definition.ExecutionStatus].(json.Number).Int64()
		historyLen, _ := source[definition.HistoryLength].(json.Number).Int64()

		record := &p.VisibilityWorkflowExecutionInfo{
			WorkflowID:    source[definition.WorkflowID].(string),
			RunID:         source[definition.RunID].(string),
			TypeName:      source[definition.WorkflowType].(string),
			StartTime:     time.Unix(0, startTime),
			ExecutionTime: time.Unix(0, executionTime),
			TaskQueue:     source[definition.TaskQueue].(string),
			Memo:          p.NewDataBlob([]byte(source[definition.Memo].(string)), source[definition.Encoding].(string)),
		}
		record.CloseTime = time.Unix(0, closeTime)
		statusEnum := enumspb.WorkflowExecutionStatus(status)
		record.Status = statusEnum
		record.HistoryLength = historyLen
	}
}
