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

package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSerializePageToken(t *testing.T) {
	s := assert.New(t)

	token := pageToken{
		CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
		StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
		RunID:     "test-run-id",
	}
	data, err := serializePageToken(&token)
	s.NoError(err)
	s.Equal(
		[]byte(`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id"}`),
		data,
	)
}

func TestDeserializePageToken(t *testing.T) {
	s := assert.New(t)

	token, err := deserializePageToken(nil)
	s.NoError(err)
	s.Nil(token)

	token, err = deserializePageToken([]byte{})
	s.NoError(err)
	s.Nil(token)

	token, err = deserializePageToken(
		[]byte(`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id"}`),
	)
	s.NoError(err)
	s.NotNil(token)
	s.Equal(
		pageToken{
			CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
			StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
			RunID:     "test-run-id",
		},
		*token,
	)
}
