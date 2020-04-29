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
	"errors"
	"sync"
)

var (
	// ErrIteratorFinished indicates that Next was called on an iterator
	// which has already reached the end of its input.
	ErrIteratorFinished = errors.New("iterator has reached end")
)

type (
	// Iterator is used to iterate over entities.
	// Each entity is represented as a byte slice.
	// Iterator will fetch pages using the provided GetFn.
	// Pages will be fetched starting from provided minPage and continuing until provided maxPage.
	// Iterator will skip over any pages with empty input and will skip over any empty elements within a page.
	// Iterator is thread safe and makes deep copies of all in and out data.
	// If Next returns an error all subsequent calls to Next will return the same error.
	Iterator interface {
		Next() ([]byte, error)
		HasNext() bool
	}

	// GetFn fetches bytes for given page number. Returns error on failure.
	GetFn func(int) ([]byte, error)

	iterator struct {
		sync.Mutex

		currentPage int
		page        [][]byte
		pageIndex   int
		nextResult  []byte
		nextError   error

		separatorToken []byte
		getFn          GetFn
		minPage        int
		maxPage        int
	}
)

// NewIterator constructs a new iterator.
func NewIterator(
	minPage int,
	maxPage int,
	getFn GetFn,
	separatorToken []byte,
) Iterator {
	separatorTokenCopy := make([]byte, len(separatorToken), len(separatorToken))
	copy(separatorTokenCopy, separatorToken)
	itr := &iterator{
		currentPage: -1,

		separatorToken: separatorTokenCopy,
		getFn:          getFn,
		minPage:        minPage,
		maxPage:        maxPage,
	}
	itr.advance(true)
	return itr
}

// Next returns the next element in the iterator.
// Returns an error if no elements are left or if a non-recoverable error occurred.
func (i *iterator) Next() ([]byte, error) {
	i.Lock()
	defer i.Unlock()

	result := i.nextResult
	error := i.nextError

	i.advance(false)

	copyResult := make([]byte, len(result), len(result))
	copy(copyResult, result)
	return copyResult, error
}

// HasNext returns true if next invocation of Next will return on-empty byte blob and nil error, false otherwise.
func (i *iterator) HasNext() bool {
	i.Lock()
	defer i.Unlock()

	return i.hasNext()
}

func (i *iterator) advance(initialization bool) {
	if !i.hasNext() && !initialization {
		return
	}
	i.advanceOnce()
	for len(i.nextResult) == 0 && i.nextError == nil {
		i.advanceOnce()
	}
}

func (i *iterator) advanceOnce() {
	if i.pageIndex < len(i.page) {
		i.consumeFromCurrentPage()
		return
	}
	if i.currentPage >= i.maxPage {
		i.setIteratorToTerminalState(ErrIteratorFinished)
		return
	}
	i.page = nil
	i.currentPage++
	i.pageIndex = 0
	data, err := i.getFn(i.currentPage)
	if err != nil {
		i.setIteratorToTerminalState(err)
		return
	}
	if len(data) == 0 {
		i.nextResult = nil
		i.nextError = nil
	} else {
		copyData := make([]byte, len(data), len(data))
		copy(copyData, data)
		i.page = bytes.Split(copyData, i.separatorToken)
		i.consumeFromCurrentPage()
	}
}

func (i *iterator) consumeFromCurrentPage() {
	i.nextResult = i.page[i.pageIndex]
	i.nextError = nil
	i.pageIndex = i.pageIndex + 1
}

func (i *iterator) setIteratorToTerminalState(err error) {
	i.nextResult = nil
	i.nextError = err
}

func (i *iterator) hasNext() bool {
	return len(i.nextResult) > 0 && i.nextError == nil
}
