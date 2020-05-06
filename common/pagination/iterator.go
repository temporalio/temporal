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
	iterator struct {
		page        Page
		entityIndex int

		nextEntity Entity
		nextError  error

		fetchFn FetchFn
	}
)

// NewIterator constructs a new Iterator
func NewIterator(
	startingPageToken PageToken,
	fetchFn FetchFn,
) Iterator {
	itr := &iterator{
		page: Page{
			Entities:     nil,
			CurrentToken: nil,
			NextToken:    startingPageToken,
		},
		entityIndex: 0,
		fetchFn:     fetchFn,
	}
	itr.advance(true)
	return itr
}

// Next returns the next Entity or error.
// Returning nil, nil is valid if that is what the provided fetch function provided.
func (i *iterator) Next() (Entity, error) {
	entity := i.nextEntity
	error := i.nextError
	i.advance(false)
	return entity, error
}

// HasNext returns true if there is a next element. There is considered to be a next element
// As long as a fatal error has not occurred and the iterator has not reached the end.
func (i *iterator) HasNext() bool {
	return i.nextError == nil
}

func (i *iterator) advance(firstPage bool) {
	if !i.HasNext() && !firstPage {
		return
	}
	if i.entityIndex < len(i.page.Entities) {
		i.consume()
	} else {
		if err := i.advanceToNonEmptyPage(firstPage); err != nil {
			i.terminate(err)
		} else {
			i.consume()
		}
	}
}

func (i *iterator) advanceToNonEmptyPage(firstPage bool) error {
	if i.page.NextToken == nil && !firstPage {
		return ErrIteratorFinished
	}
	nextPage, err := i.fetchFn(i.page.NextToken)
	if err != nil {
		return err
	}
	i.page = nextPage
	if len(i.page.Entities) != 0 {
		i.entityIndex = 0
		return nil
	}
	return i.advanceToNonEmptyPage(false)
}

func (i *iterator) consume() {
	i.nextEntity = i.page.Entities[i.entityIndex]
	i.nextError = nil
	i.entityIndex++
}

func (i *iterator) terminate(err error) {
	i.nextEntity = nil
	i.nextError = err
}
