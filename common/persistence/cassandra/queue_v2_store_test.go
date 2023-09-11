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

// This file contains unit tests for the queue v2 implementation. However, the majority of the test cases warrant an
// actual database and are located in the persistencetests package.

package cassandra_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type failingSession struct {
	gocql.Session
}

func (f failingSession) Query(string, ...interface{}) gocql.Query {
	return failingQuery{}
}

type failingQuery struct {
	gocql.Query
}

func (f failingQuery) WithContext(context.Context) gocql.Query {
	return f
}

func (f failingQuery) Scan(...interface{}) error {
	return assert.AnError
}

func TestGetMaxQueueMessageIDQueryErr(t *testing.T) {
	t.Parallel()

	q := cassandra.NewQueueV2Store(failingSession{})
	_, err := q.EnqueueMessage(context.Background(), &persistence.InternalEnqueueMessageRequest{
		QueueType: persistence.QueueTypeHistoryNormal,
		QueueName: "test-queue-" + t.Name(),
		Blob: commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_JSON,
			Data:         []byte("1"),
		},
	})
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2GetMaxMessageID")
}
