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

import "encoding/json"

type (
	jsonHistoryTokenSerializer struct{}

	// historyV2PagingToken is used to serialize/deserialize pagination token for ReadHistoryBranchRequest
	historyV2PagingToken struct {
		LastEventVersion int64
		LastEventID      int64
		// the pagination token passing to persistence
		StoreToken []byte
		// recording which branchRange it is reading
		CurrentRangeIndex int
		FinalRangeIndex   int
	}
)

const notStartedIndex = -1

// newJSONHistoryTokenSerializer creates a new instance of TaskTokenSerializer
func newJSONHistoryTokenSerializer() *jsonHistoryTokenSerializer {
	return &jsonHistoryTokenSerializer{}
}

func (t *historyV2PagingToken) SetRangeIndexes(curr, final int) {
	t.CurrentRangeIndex = curr
	t.FinalRangeIndex = final
}

func (j *jsonHistoryTokenSerializer) Serialize(token *historyV2PagingToken) ([]byte, error) {
	data, err := json.Marshal(token)
	return data, err
}

func (j *jsonHistoryTokenSerializer) Deserialize(data []byte, defaultLastNodeID, defaultLastEventVersion int64) (*historyV2PagingToken, error) {
	if len(data) == 0 {
		token := historyV2PagingToken{
			LastEventVersion:  defaultLastEventVersion,
			LastEventID:       defaultLastNodeID,
			CurrentRangeIndex: notStartedIndex,
		}
		return &token, nil
	}

	token := historyV2PagingToken{}
	err := json.Unmarshal(data, &token)
	return &token, err
}
