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

package replicationadmin

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
)

func PurgeDLQ(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
	shard shard.Context,
	replicationDLQHandler replication.DLQHandler,
) (*historyservice.PurgeDLQMessagesResponse, error) {
	_, ok := shard.GetClusterMetadata().GetAllClusterInfo()[request.GetSourceCluster()]
	if !ok {
		return nil, consts.ErrUnknownCluster
	}

	if err := replicationDLQHandler.PurgeMessages(
		ctx,
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageId(),
	); err != nil {
		return nil, err
	}
	return &historyservice.PurgeDLQMessagesResponse{}, nil
}
