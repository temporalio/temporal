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

package sql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.temporal.io/server/common/persistence/sql"
)

func TestNewQueueV2(t *testing.T) {
	t.Parallel()

	q := sql.NewQueueV2()
	_, err := q.EnqueueMessage(context.Background(), nil)
	if assert.ErrorIs(t, err, sql.ErrNotImplemented) {
		assert.ErrorContains(t, err, "EnqueueMessage")
	}
	_, err = q.ReadMessages(context.Background(), nil)
	if assert.ErrorIs(t, err, sql.ErrNotImplemented) {
		assert.ErrorContains(t, err, "ReadMessages")
	}
	_, err = q.CreateQueue(context.Background(), nil)
	if assert.ErrorIs(t, err, sql.ErrNotImplemented) {
		assert.ErrorContains(t, err, "CreateQueue")
	}
	_, err = q.RangeDeleteMessages(context.Background(), nil)
	if assert.ErrorIs(t, err, sql.ErrNotImplemented) {
		assert.ErrorContains(t, err, "RangeDeleteMessages")
	}
}
