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

package elasticsearch

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	p "github.com/uber/cadence/common/persistence"
)

var (
	data = []byte(`{"CloseStatus": 0,
         "CloseTime": 1547596872817380000,
         "DomainID": "bfd5c907-f899-4baf-a7b2-2ab85e623ebd",
         "HistoryLength": 29,
         "KafkaKey": "7-619",
         "RunID": "e481009e-14b3-45ae-91af-dce6e2a88365",
         "StartTime": 1547596872371000000,
         "WorkflowID": "6bfbc1e5-6ce4-4e22-bbfb-e0faa9a7a604-1-2256",
         "WorkflowType": "TestWorkflowExecute",
 		 "Encoding" : "thriftrw",
 	     "Memo" : "WQ0ACgsLAAAAAwAAAAJrMgAAAAkidmFuY2V4dSIAAAACazMAAAADMTIzAAAAAmsxAAAAUXsia2V5MSI6MTIzNDMyMSwia2V5MiI6ImEgc3RyaW5nIGlzIHZlcnkgbG9uZyIsIm1hcCI6eyJtS2V5IjoxMjM0MywiYXNkIjoiYXNkZiJ9fQA="}`)
)

/*
BenchmarkJSONDecodeToType-8       200000              9321 ns/op
BenchmarkJSONDecodeToMap-8        100000             12878 ns/op
*/

//nolint
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
			Memo:          p.NewDataBlob(source.Memo, common.EncodingType(source.Encoding)),
		}
		record.CloseTime = time.Unix(0, source.CloseTime)
		record.Status = &source.CloseStatus
		record.HistoryLength = source.HistoryLength
	}
}

//nolint
func BenchmarkJSONDecodeToMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var source map[string]interface{}
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber()
		d.Decode(&source)

		startTime, _ := source[definition.StartTime].(json.Number).Int64()
		executionTime, _ := source[definition.StartTime].(json.Number).Int64()
		closeTime, _ := source[definition.CloseTime].(json.Number).Int64()
		closeStatus, _ := source[definition.CloseStatus].(json.Number).Int64()
		historyLen, _ := source[definition.HistoryLength].(json.Number).Int64()

		record := &p.VisibilityWorkflowExecutionInfo{
			WorkflowID:    source[definition.WorkflowID].(string),
			RunID:         source[definition.RunID].(string),
			TypeName:      source[definition.WorkflowType].(string),
			StartTime:     time.Unix(0, startTime),
			ExecutionTime: time.Unix(0, executionTime),
			Memo:          p.NewDataBlob([]byte(source[definition.Memo].(string)), common.EncodingType(source[definition.Encoding].(string))),
		}
		record.CloseTime = time.Unix(0, closeTime)
		status := (shared.WorkflowExecutionCloseStatus)(int32(closeStatus))
		record.Status = &status
		record.HistoryLength = historyLen
	}
}
