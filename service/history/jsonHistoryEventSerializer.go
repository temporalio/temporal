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

package history

import (
	"encoding/json"

	workflow "github.com/uber/cadence/.gen/go/shared"
)

type (
	jsonHistoryEventSerializer struct {
	}
)

func newJSONHistoryEventSerializer() historyEventSerializer {
	return &jsonHistoryEventSerializer{}
}

func (j *jsonHistoryEventSerializer) Serialize(event *workflow.HistoryEvent) ([]byte, error) {
	data, err := json.Marshal(event)

	return data, err
}

func (j *jsonHistoryEventSerializer) Deserialize(data []byte) (*workflow.HistoryEvent, error) {
	var history *workflow.HistoryEvent
	err := json.Unmarshal(data, &history)

	return history, err
}
