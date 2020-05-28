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

type (
	writer struct {
		writeFn       WriteFn
		shouldFlushFn ShouldFlushFn
		flushedPages  []PageToken
		page          Page
	}
)

// NewWriter constructs a new Writer
func NewWriter(
	writeFn WriteFn,
	shouldFlushFn ShouldFlushFn,
	startingPage PageToken,
) Writer {
	return &writer{
		writeFn:       writeFn,
		shouldFlushFn: shouldFlushFn,
		flushedPages:  nil,
		page: Page{
			Entities:     nil,
			CurrentToken: startingPage,
		},
	}
}

// Add adds entity to buffer and flushes if provided shouldFlushFn indicates the page should be flushed.
func (w *writer) Add(e Entity) error {
	w.page.Entities = append(w.page.Entities, e)
	if !w.shouldFlushFn(w.page) {
		return nil
	}
	return w.Flush()
}

// Flush flushes the buffer.
func (w *writer) Flush() error {
	nextPageToken, err := w.writeFn(w.page)
	if err != nil {
		return err
	}
	w.flushedPages = append(w.flushedPages, w.page.CurrentToken)
	w.page = Page{
		Entities:     nil,
		CurrentToken: nextPageToken,
	}
	return nil
}

// FlushIfNotEmpty flushes the buffer if and only if it is not empty
func (w *writer) FlushIfNotEmpty() error {
	if len(w.page.Entities) == 0 {
		return nil
	}
	return w.Flush()
}

// FlushedPages returns all pages which have been successfully flushed.
func (w *writer) FlushedPages() []PageToken {
	return w.flushedPages
}

// FirstFlushedPage returns the first page that was flushed or nil if no pages have been flushed.
func (w *writer) FirstFlushedPage() PageToken {
	if len(w.flushedPages) == 0 {
		return nil
	}
	return w.flushedPages[0]
}

// LastFlushedPage returns the last page that was flushed or nil if no pages have been flushed
func (w *writer) LastFlushedPage() PageToken {
	if len(w.flushedPages) == 0 {
		return nil
	}
	return w.flushedPages[len(w.flushedPages)-1]
}
