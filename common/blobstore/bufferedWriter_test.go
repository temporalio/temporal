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

package blobstore

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type BufferedWriterSuite struct {
	*require.Assertions
	suite.Suite
}

func TestBufferedWriterSuite(t *testing.T) {
	suite.Run(t, new(BufferedWriterSuite))
}

func (s *BufferedWriterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *BufferedWriterSuite) TestGetKeysCopy() {
	bw := &bufferedWriter{
		keys: []string{"key1", "key2", "key3"},
	}
	copy := bw.getKeysCopy()
	s.Equal(copy, bw.keys)
	copy[0] = "changed_key"
	s.NotEqual(copy, bw.keys)
}

func (s *BufferedWriterSuite) TestConstructBlob() {
	bw := &bufferedWriter{
		currentBody: bytes.NewBuffer([]byte{1, 2, 3}),
		currentTags: map[string]string{"key1": "value1", "key2": "value2"},
	}
	blob := bw.constructBlob()
	s.Equal(bw.currentBody.Bytes(), blob.Body)
	s.Equal(bw.currentTags, blob.Tags)
	blob.Body[0] = 0
	blob.Tags["not_exists_key"] = "not_exists_value"
	s.NotEqual(bw.currentBody.Bytes(), blob.Body)
	s.NotEqual(bw.currentTags, blob.Tags)
}

func (s *BufferedWriterSuite) TestAdvancePage() {
	bw := &bufferedWriter{
		currentBody: bytes.NewBuffer([]byte{1, 2, 3}),
		currentTags: map[string]string{"key1": "value1", "key2": "value2"},
		currentPage: 10,
	}
	bw.advancePage()
	s.Len(bw.currentBody.Bytes(), 0)
	s.Len(bw.currentTags, 0)
	s.Equal(11, bw.currentPage)
}

func (s *BufferedWriterSuite) TestShouldFlush() {
	bw := &bufferedWriter{
		currentBody:    bytes.NewBuffer([]byte{1, 2, 3}),
		flushThreshold: 10,
	}
	s.False(bw.shouldFlush())
	bw.flushThreshold = 3
	s.True(bw.shouldFlush())
}

func (s *BufferedWriterSuite) TestWriteToBody() {
	bw := &bufferedWriter{
		currentBody:    &bytes.Buffer{},
		separatorToken: []byte("\r\n"),
	}
	s.Error(bw.writeToBody(make(chan struct{})))
	s.Equal("", bw.currentBody.String())

	s.NoError(bw.writeToBody("first"))
	s.Error(bw.writeToBody(make(chan struct{})))
	s.Equal("\"first\"\r\n", bw.currentBody.String())
}

func (s *BufferedWriterSuite) TestFlush_PutReturnsError() {
	putFn := func(blob Blob, page int) (string, error) {
		return "", errors.New("put function returns error")
	}
	bw := &bufferedWriter{
		currentBody: bytes.NewBuffer([]byte{1, 2, 3}),
		currentTags: map[string]string{"key1": "value1"},
		currentPage: 1,

		keys: []string{"page_0"},

		putFn: putFn,
	}
	s.Error(bw.flush())
	s.Equal(1, bw.currentPage)
	s.Equal([]byte{1, 2, 3}, bw.currentBody.Bytes())
	s.Equal(map[string]string{"key1": "value1"}, bw.currentTags)
}

func (s *BufferedWriterSuite) TestFlush_Success() {
	putFn := func(blob Blob, page int) (string, error) {
		return fmt.Sprintf("page_%v", page), nil
	}
	bw := &bufferedWriter{
		currentBody: bytes.NewBuffer([]byte{1, 2, 3}),
		currentTags: map[string]string{"key1": "value1"},
		currentPage: 1,

		keys: []string{"page_0"},

		putFn: putFn,
	}
	s.NoError(bw.flush())
	s.Equal(2, bw.currentPage)
	s.Len(bw.currentBody.Bytes(), 0)
	s.Equal(map[string]string{}, bw.currentTags)
	s.Equal([]string{"page_0", "page_1"}, bw.keys)
}

func (s *BufferedWriterSuite) TestAddAndFlush() {
	putFn := func(blob Blob, page int) (string, error) {
		return fmt.Sprintf("page_%v", page), nil
	}
	bw := NewBufferedWriter(putFn, 10, []byte("\r\n"), 0).(*bufferedWriter)
	for i := 0; i < 100; i++ {
		// this will add three bytes one for the 0 and two for the separator
		flushed, err := bw.AddEntity(0)
		s.NoError(err)
		if (i+1)%4 == 0 {
			s.True(flushed)
		} else {
			s.False(flushed)
		}
	}
	s.Len(bw.keys, 25)
	s.Equal(25, bw.currentPage)
	s.Len(bw.currentBody.Bytes(), 0)
}
