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

package pagination

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TestEntity struct {
	Counter int
}

var separator = []byte("\r\n")

type WriterIteratorSuite struct {
	*require.Assertions
	suite.Suite
}

func TestWriterIteratorSuite(t *testing.T) {
	suite.Run(t, new(WriterIteratorSuite))
}

func (s *WriterIteratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *WriterIteratorSuite) TestWriterIterator() {
	store := make(map[string][]byte)

	shouldFlushFn := func(page Page) bool {
		return len(page.Entities) == 10
	}
	pageNum := 0
	writeFn := func(page Page) (PageToken, error) {
		buffer := &bytes.Buffer{}
		for _, e := range page.Entities {
			data, err := json.Marshal(e)
			if err != nil {
				return nil, err
			}
			buffer.Write(data)
			buffer.Write(separator)
		}
		store[page.CurrentToken.(string)] = buffer.Bytes()
		pageNum++
		return fmt.Sprintf("key_%v", pageNum), nil
	}
	writer := NewWriter(writeFn, shouldFlushFn, fmt.Sprintf("key_%v", pageNum))
	for i := 0; i < 100; i++ {
		te := TestEntity{
			Counter: i,
		}
		s.NoError(writer.Add(te))
	}
	flushedKeys := writer.FlushedPages()
	s.Len(flushedKeys, 10)
	for i := 0; i < 10; i++ {
		expectedKey := fmt.Sprintf("key_%v", i)
		s.Equal(expectedKey, flushedKeys[i].(string))
	}

	fetchFn := func(token PageToken) (Page, error) {
		key := flushedKeys[token.(int)]
		data := store[key.(string)]
		dataBlobs := bytes.Split(data, separator)
		var entities []Entity
		for _, db := range dataBlobs {
			if len(db) == 0 {
				continue
			}
			var entity TestEntity
			if err := json.Unmarshal(db, &entity); err != nil {
				return Page{}, err
			}
			entities = append(entities, entity)
		}
		var nextPageToken interface{} = token.(int) + 1
		if nextPageToken == len(flushedKeys) {
			nextPageToken = nil
		}
		return Page{
			CurrentToken: token,
			NextToken:    nextPageToken,
			Entities:     entities,
		}, nil
	}

	itr := NewIterator(0, fetchFn)
	itrCount := 0
	for itr.HasNext() {
		val, err := itr.Next()
		s.NoError(err)
		te := val.(TestEntity)
		s.Equal(itrCount, te.Counter)
		itrCount++
	}
	s.Equal(100, itrCount)
}
