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

package archiver

import (
	"errors"
)

/**
IMPORTANT: Under the assumption that history is immutable, the following reader will deterministically return identical
blobs from GetBlob, for the same pageToken, regardless of the current cluster and regardless of the number of times this call is made.
This property makes any concurrent uploads of history safe.
*/

type (
	// HistoryBlobReader is used to read history blobs
	HistoryBlobReader interface {
		GetBlob(pageToken int) (*HistoryBlob, error)
	}

	historyBlobReader struct {
		itr   HistoryBlobIterator
		cache map[int]*HistoryBlob
	}
)

var (
	errPageTokenOutOfBounds = errors.New("requested blob with page token which is greater than largest page token for history")
)

// NewHistoryBlobReader returns a new HistoryBlobReader
func NewHistoryBlobReader(itr HistoryBlobIterator) HistoryBlobReader {
	return &historyBlobReader{
		itr:   itr,
		cache: make(map[int]*HistoryBlob),
	}
}

// GetBlob returns the HistoryBlob with given pageToken
func (r *historyBlobReader) GetBlob(pageToken int) (*HistoryBlob, error) {
	result, ok := r.cache[pageToken]
	if ok {
		return result, nil
	}
	for r.itr.HasNext() {
		hb, err := r.itr.Next()
		if err != nil {
			return nil, err
		}
		r.cache[*hb.Header.CurrentPageToken] = hb
		result, ok = r.cache[pageToken]
		if ok {
			return result, nil
		}
	}
	return nil, errPageTokenOutOfBounds
}
