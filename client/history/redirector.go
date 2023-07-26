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

package history

import (
	"context"
	"errors"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/membership"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	// A redirector executes a client operation against a history instance.
	// If the operation is intended for the owner of a shard, and the request
	// returns a shard ownership lost error with a hint for a new shard owner,
	// the redirector will retry the request to the new owner.
	redirector interface {
		clientForShardID(int32) (historyservice.HistoryServiceClient, error)
		execute(context.Context, int32, clientOperation) error
	}

	clientOperation func(ctx context.Context, client historyservice.HistoryServiceClient) error

	basicRedirector struct {
		connections            connectionPool
		historyServiceResolver membership.ServiceResolver
	}
)

func shardLookup(resolver membership.ServiceResolver, shardID int32) (rpcAddress, error) {
	hostInfo, err := resolver.Lookup(convert.Int32ToString(shardID))
	if err != nil {
		return "", err
	}
	return rpcAddress(hostInfo.GetAddress()), nil
}

func newBasicRedirector(
	connections connectionPool,
	historyServiceResolver membership.ServiceResolver,
) *basicRedirector {
	return &basicRedirector{
		connections:            connections,
		historyServiceResolver: historyServiceResolver,
	}
}

func (r *basicRedirector) clientForShardID(shardID int32) (historyservice.HistoryServiceClient, error) {
	if err := checkShardID(shardID); err != nil {
		return nil, err
	}
	address, err := shardLookup(r.historyServiceResolver, shardID)
	if err != nil {
		return nil, err
	}
	clientConn := r.connections.getOrCreateClientConn(address)
	return clientConn.historyClient, nil
}

func (r *basicRedirector) execute(ctx context.Context, shardID int32, op clientOperation) error {
	if err := checkShardID(shardID); err != nil {
		return err
	}
	address, err := shardLookup(r.historyServiceResolver, shardID)
	if err != nil {
		return err
	}
	return r.redirectLoop(ctx, address, op)
}

func (r *basicRedirector) redirectLoop(ctx context.Context, address rpcAddress, op clientOperation) error {
	for {
		if err := common.IsValidContext(ctx); err != nil {
			return err
		}
		clientConn := r.connections.getOrCreateClientConn(address)
		err := op(ctx, clientConn.historyClient)
		var solErr *serviceerrors.ShardOwnershipLost
		if !errors.As(err, &solErr) || len(solErr.OwnerHost) == 0 {
			return err
		}
		// TODO: consider emitting a metric for number of redirects
		address = rpcAddress(solErr.OwnerHost)
	}
}
