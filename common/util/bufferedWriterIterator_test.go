// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package util

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferedWriterWithIterator(t *testing.T) {
	blobMap := make(map[string][]byte)
	handleFn := func(data []byte, page int) error {
		key := fmt.Sprintf("key_%v", page)
		blobMap[key] = data
		return nil
	}
	bw := NewBufferedWriter(handleFn, json.Marshal, 100, []byte("\r\n"))
	for i := 0; i < 1000; i++ {
		assert.NoError(t, bw.Add(i))
	}
	assert.NoError(t, bw.Flush())
	lastFlushedPage := bw.LastFlushedPage()
	getFn := func(page int) ([]byte, error) {
		key := fmt.Sprintf("key_%v", page)
		return blobMap[key], nil
	}
	itr := NewIterator(0, lastFlushedPage, getFn, []byte("\r\n"))
	i := 0
	for itr.HasNext() {
		val, err := itr.Next()
		assert.NoError(t, err)
		expectedVal, err := json.Marshal(i)
		assert.NoError(t, err)
		assert.Equal(t, expectedVal, val)
		i++
	}
}
