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
	"bytes"
	"encoding/json"
	"errors"
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

func (s *BufferedWriterSuite) TestShouldFlush() {
	bw := &bufferedWriter{
		buffer:         bytes.NewBuffer([]byte{1, 2, 3}),
		flushThreshold: 10,
	}
	s.False(bw.shouldFlush())
	bw.flushThreshold = 3
	s.True(bw.shouldFlush())
}

func (s *BufferedWriterSuite) TestWriteToBuffer() {
	bw := &bufferedWriter{
		buffer:         &bytes.Buffer{},
		separatorToken: []byte("\r\n"),
		serializeFn:    json.Marshal,
	}
	s.Error(bw.writeToBuffer(make(chan struct{})))
	s.Equal("", bw.buffer.String())

	s.NoError(bw.writeToBuffer("first"))
	s.Error(bw.writeToBuffer(make(chan struct{})))
	s.Equal("\"first\"\r\n", bw.buffer.String())
}

func (s *BufferedWriterSuite) TestFlush_HandleReturnsError() {
	handleFn := func(data []byte, page int) error {
		return errors.New("put function returns error")
	}
	bw := &bufferedWriter{
		buffer:   bytes.NewBuffer([]byte{1, 2, 3}),
		page:     1,
		handleFn: handleFn,
	}
	s.Error(bw.flush())
	s.Equal(1, bw.page)
	s.Equal([]byte{1, 2, 3}, bw.buffer.Bytes())
}

func (s *BufferedWriterSuite) TestFlush_Success() {
	handleFn := func(data []byte, page int) error {
		return nil
	}
	bw := &bufferedWriter{
		buffer:   bytes.NewBuffer([]byte{1, 2, 3}),
		page:     1,
		handleFn: handleFn,
	}
	s.NoError(bw.flush())
	s.Len(bw.buffer.Bytes(), 0)
	s.Equal(1, bw.LastFlushedPage())
}

func (s *BufferedWriterSuite) TestAddAndFlush() {
	handleFn := func(data []byte, page int) error {
		return nil
	}
	bw := NewBufferedWriter(handleFn, json.Marshal, 10, []byte("\r\n")).(*bufferedWriter)
	expectedLastFlushedPage := -1
	for i := 1; i <= 100; i++ {
		s.NoError(bw.Add(0))
		if i%4 == 0 {
			expectedLastFlushedPage++
		}
		s.Equal(expectedLastFlushedPage, bw.LastFlushedPage())
	}
	s.Equal(24, bw.LastFlushedPage())
	s.Len(bw.buffer.Bytes(), 0)
}
