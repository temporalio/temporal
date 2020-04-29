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
	"sync"
)

type (
	// BufferedWriter is used to buffer entities, construct byte blobs and invoke handle function on byte blobs.
	// BufferedWriter is thread safe and makes defensive copies in and out.
	// BufferedWriter's state is unchanged whenever any method returns an error.
	BufferedWriter interface {
		Add(interface{}) error
		Flush() error
		LastFlushedPage() int
	}

	// HandleFn is invoked whenever a byte blob needs to be flushed.
	// Takes in deep copy of constructed byte blob and the current page number.
	// Returns an error on failure to handle or nil otherwise.
	HandleFn func([]byte, int) error

	// SerializeFn is used to serialize entities.
	SerializeFn func(interface{}) ([]byte, error)

	bufferedWriter struct {
		sync.Mutex

		buffer *bytes.Buffer
		page   int

		flushThreshold int
		separatorToken []byte
		handleFn       HandleFn
		serializeFn    SerializeFn
	}
)

// NewBufferedWriter constructs a new BufferedWriter
func NewBufferedWriter(
	handleFn HandleFn,
	serializeFn SerializeFn,
	flushThreshold int,
	separatorToken []byte,
) BufferedWriter {
	separatorTokenCopy := make([]byte, len(separatorToken), len(separatorToken))
	copy(separatorTokenCopy, separatorToken)
	return &bufferedWriter{
		buffer: &bytes.Buffer{},
		page:   0,

		flushThreshold: flushThreshold,
		separatorToken: separatorTokenCopy,
		handleFn:       handleFn,
		serializeFn:    serializeFn,
	}
}

// Add adds element to buffer. Triggers flush if exceeds flushThreshold.
func (bw *bufferedWriter) Add(e interface{}) error {
	bw.Lock()
	defer bw.Unlock()

	if err := bw.writeToBuffer(e); err != nil {
		return err
	}
	if bw.shouldFlush() {
		if err := bw.flush(); err != nil {
			return err
		}
	}
	return nil
}

// Flush invokes PutFn and advances state of bufferedWriter to next page.
func (bw *bufferedWriter) Flush() error {
	bw.Lock()
	defer bw.Unlock()

	return bw.flush()
}

// LastFlushedPage returns the page number of the last page that was flushed.
// Returns -1 if no pages have been flushed.
func (bw *bufferedWriter) LastFlushedPage() int {
	bw.Lock()
	defer bw.Unlock()

	return bw.page - 1
}

func (bw *bufferedWriter) flush() error {
	src := bw.buffer.Bytes()
	dest := make([]byte, len(src), len(src))
	copy(dest, src)
	err := bw.handleFn(dest, bw.page)
	if err != nil {
		return err
	}
	bw.startNewPage()
	return nil
}

func (bw *bufferedWriter) writeToBuffer(e interface{}) error {
	data, err := bw.serializeFn(e)
	if err != nil {
		return err
	}

	// write will never return an error, so it can be safely ignored
	bw.buffer.Write(data)
	bw.buffer.Write(bw.separatorToken)
	return nil
}

func (bw *bufferedWriter) shouldFlush() bool {
	return bw.buffer.Len() >= bw.flushThreshold
}

func (bw *bufferedWriter) startNewPage() {
	bw.buffer = &bytes.Buffer{}
	bw.page = bw.page + 1
}
