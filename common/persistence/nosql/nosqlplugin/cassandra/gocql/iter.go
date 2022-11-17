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

package gocql

import (
	"github.com/gocql/gocql"
)

type iter struct {
	session   *session
	gocqlIter *gocql.Iter
}

func newIter(session *session, gocqlIter *gocql.Iter) *iter {
	return &iter{
		session:   session,
		gocqlIter: gocqlIter,
	}
}

func (it *iter) Scan(dest ...interface{}) bool {
	return it.gocqlIter.Scan(dest...)
}

func (it *iter) MapScan(m map[string]interface{}) bool {
	return it.gocqlIter.MapScan(m)
}

func (it *iter) PageState() []byte {
	return it.gocqlIter.PageState()
}

func (it *iter) Close() (retError error) {
	defer func() { it.session.handleError(retError) }()

	return it.gocqlIter.Close()
}
