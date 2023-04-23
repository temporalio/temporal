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

package readrawhistorybranch

import (
	"context"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	req *historyservice.ReadRawHistoryBranchRequest,
	shardCtx shard.Context,
) (_ *historyservice.ReadRawHistoryBranchResponse, retErr error) {
	res, err := shardCtx.GetExecutionManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		ShardID:       req.ShardId,
		BranchToken:   req.BranchToken,
		MinEventID:    req.MinEventId,
		MaxEventID:    req.MaxEventId,
		PageSize:      int(req.PageSize),
		NextPageToken: req.NextPageToken,
	})
	if err != nil {
		return nil, err
	}
	var blobs []*common.DataBlob
	for _, blob := range res.HistoryEventBlobs {
		blobs = append(blobs, &common.DataBlob{
			EncodingType: blob.EncodingType,
			Data:         blob.Data,
		})
	}
	return &historyservice.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: blobs,
		NodeIds:           res.NodeIDs,
		NextPageToken:     res.NextPageToken,
		Size_:             int64(res.Size),
	}, nil
}
