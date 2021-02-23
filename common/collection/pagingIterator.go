// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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

package collection

type (
	// PaginationFn is the function which get a page of results
	PaginationFn func(paginationToken []byte) ([]interface{}, []byte, error)

	// PagingIteratorImpl is the implementation of PagingIterator
	PagingIteratorImpl struct {
		paginationFn      PaginationFn
		pageToken         []byte
		pageErr           error
		pageItems         []interface{}
		nextPageItemIndex int
	}
)

// NewPagingIterator create a new paging iterator
func NewPagingIterator(
	paginationFn PaginationFn,
) Iterator {
	iter := &PagingIteratorImpl{
		paginationFn:      paginationFn,
		pageToken:         nil,
		pageErr:           nil,
		pageItems:         nil,
		nextPageItemIndex: 0,
	}
	iter.getNextPage() // this will initialize the paging iterator
	return iter
}

// NewPagingIteratorWithToken create a new paging iterator with initial token
func NewPagingIteratorWithToken(
	paginationFn PaginationFn,
	pageToken []byte,
) Iterator {
	iter := &PagingIteratorImpl{
		paginationFn:      paginationFn,
		pageToken:         pageToken,
		pageErr:           nil,
		pageItems:         nil,
		nextPageItemIndex: 0,
	}
	iter.getNextPage() // this will initialize the paging iterator
	return iter
}

// HasNext return whether has next item or err
func (iter *PagingIteratorImpl) HasNext() bool {
	// pagination encounters error
	if iter.pageErr != nil {
		return true
	}

	// still have local cached item to return
	if iter.nextPageItemIndex < len(iter.pageItems) {
		return true
	}

	if len(iter.pageToken) != 0 {
		iter.getNextPage()
		return iter.HasNext()
	}

	return false
}

// Next return next item or err
func (iter *PagingIteratorImpl) Next() (interface{}, error) {
	if !iter.HasNext() {
		panic("HistoryEventIterator Next() called without checking HasNext()")
	}

	if iter.pageErr != nil {
		err := iter.pageErr
		iter.pageErr = nil
		return nil, err
	}

	// we have cached events
	if iter.nextPageItemIndex < len(iter.pageItems) {
		index := iter.nextPageItemIndex
		iter.nextPageItemIndex++
		return iter.pageItems[index], nil
	}

	panic("HistoryEventIterator Next() should return either a history event or a err")
}

func (iter *PagingIteratorImpl) getNextPage() {
	items, token, err := iter.paginationFn(iter.pageToken)
	if err == nil {
		iter.pageItems = items
		iter.pageToken = token
		iter.pageErr = nil
	} else {
		iter.pageItems = nil
		iter.pageToken = nil
		iter.pageErr = err
	}
	iter.nextPageItemIndex = 0
}
