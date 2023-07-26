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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	serviceerrors "go.temporal.io/server/common/serviceerror"

	"go.temporal.io/server/common/membership"
)

type (
	cachingRedirectorSuite struct {
		suite.Suite
		*require.Assertions

		controller  *gomock.Controller
		connections *MockconnectionPool
		logger      log.Logger
		resolver    *membership.MockServiceResolver
	}
)

func TestCachingRedirectorSuite(t *testing.T) {
	s := new(cachingRedirectorSuite)
	suite.Run(t, s)
}

func (s *cachingRedirectorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.connections = NewMockconnectionPool(s.controller)
	s.logger = log.NewNoopLogger()
	s.resolver = membership.NewMockServiceResolver(s.controller)
}

func (s *cachingRedirectorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *cachingRedirectorSuite) TestShardCheck() {
	r := newCachingRedirector(s.connections, s.resolver, s.logger)

	invalErr := &serviceerror.InvalidArgument{}
	err := r.execute(
		context.Background(),
		-1,
		func(_ context.Context, _ historyservice.HistoryServiceClient) error {
			panic("notreached")
		})
	s.ErrorAs(err, &invalErr)

	_, err = r.clientForShardID(-1)
	s.ErrorAs(err, &invalErr)
}

func cacheRetainingTest(s *cachingRedirectorSuite, opErr error, verify func(error)) {
	testAddr := rpcAddress("testaddr")
	shardID := int32(1)

	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr)), nil).
		Times(1)

	mockClient := historyservicemock.NewMockHistoryServiceClient(s.controller)
	clientConn := clientConnection{
		historyClient: mockClient,
	}
	s.connections.EXPECT().
		getOrCreateClientConn(testAddr).
		Return(clientConn)
	s.connections.EXPECT().
		resetConnectBackoff(clientConn)

	clientOp := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		if client != mockClient {
			return errors.New("wrong client")
		}
		return opErr
	}
	r := newCachingRedirector(s.connections, s.resolver, s.logger)

	for i := 0; i < 3; i++ {
		err := r.execute(
			context.Background(),
			shardID,
			clientOp,
		)
		verify(err)
	}
}

func (s *cachingRedirectorSuite) TestExecuteShardSuccess() {
	cacheRetainingTest(s, nil, func(err error) {
		s.NoError(err)
	})
}

func (s *cachingRedirectorSuite) TestExecuteCacheRetainingError() {
	notFound := serviceerror.NewNotFound("notfound")
	cacheRetainingTest(s, notFound, func(err error) {
		s.Error(err)
		s.Equal(notFound, err)
	})
}

func hostDownErrorTest(s *cachingRedirectorSuite, clientOp clientOperation, verify func(err error)) {
	testAddr := rpcAddress("testaddr")
	shardID := int32(1)

	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr)), nil).
		Times(1)

	mockClient := historyservicemock.NewMockHistoryServiceClient(s.controller)
	clientConn := clientConnection{
		historyClient: mockClient,
	}
	s.connections.EXPECT().
		getOrCreateClientConn(testAddr).
		Return(clientConn).
		Times(1)
	s.connections.EXPECT().
		resetConnectBackoff(clientConn).
		Times(1)

	r := newCachingRedirector(s.connections, s.resolver, s.logger)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := r.execute(
		ctx,
		shardID,
		clientOp,
	)
	verify(err)
}

func (s *cachingRedirectorSuite) TestDeadlineExceededError() {
	hostDownErrorTest(s,
		func(ctx context.Context, client historyservice.HistoryServiceClient) error {
			<-ctx.Done()
			return ctx.Err()
		},
		func(err error) {
			s.ErrorIs(err, context.DeadlineExceeded)
		})
}

func (s *cachingRedirectorSuite) TestUnavailableError() {
	hostDownErrorTest(s,
		func(ctx context.Context, client historyservice.HistoryServiceClient) error {
			return serviceerror.NewUnavailable("unavail")
		},
		func(err error) {
			unavail := &serviceerror.Unavailable{}
			s.ErrorAs(err, &unavail)
		})
}

func (s *cachingRedirectorSuite) TestShardOwnershipLostErrors() {
	testAddr1 := rpcAddress("testaddr1")
	testAddr2 := rpcAddress("testaddr2")
	shardID := int32(1)

	mockClient1 := historyservicemock.NewMockHistoryServiceClient(s.controller)
	mockClient2 := historyservicemock.NewMockHistoryServiceClient(s.controller)

	r := newCachingRedirector(s.connections, s.resolver, s.logger)
	opCalls := 1
	doExecute := func() error {
		return r.execute(
			context.Background(),
			shardID,
			func(ctx context.Context, client historyservice.HistoryServiceClient) error {
				switch opCalls {
				case 1:
					if client != mockClient1 {
						return errors.New("wrong client")
					}
					opCalls++
					return serviceerrors.NewShardOwnershipLost(string(testAddr1), "current")
				case 2:
					if client != mockClient1 {
						return errors.New("wrong client")
					}
					opCalls++
					return serviceerrors.NewShardOwnershipLost("", "current")
				case 3:
					if client != mockClient1 {
						return errors.New("wrong client")
					}
					opCalls++
					return serviceerrors.NewShardOwnershipLost(string(testAddr2), "current")
				case 4:
					if client != mockClient2 {
						return errors.New("wrong client")
					}
					opCalls++
					return nil
				case 5:
					if client != mockClient2 {
						return errors.New("wrong client")
					}
					opCalls++
					return nil
				}
				return errors.New("too many op calls")
			},
		)
	}

	// opCall 1: return SOL, but with same owner as current.
	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr1)), nil).
		Times(1)

	clientConn1 := clientConnection{
		historyClient: mockClient1,
	}
	s.connections.EXPECT().
		getOrCreateClientConn(testAddr1).
		Return(clientConn1).
		Times(1)
	s.connections.EXPECT().
		resetConnectBackoff(clientConn1).
		Times(1)

	err := doExecute()
	s.Error(err)
	solErr := &serviceerrors.ShardOwnershipLost{}
	s.ErrorAs(err, &solErr)
	s.Equal(string(testAddr1), solErr.OwnerHost)

	// opCall 2: return SOL, but with empty new owner hint.
	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr1)), nil).
		Times(1)

	s.connections.EXPECT().
		getOrCreateClientConn(testAddr1).
		Return(clientConn1).
		Times(1)
	s.connections.EXPECT().
		resetConnectBackoff(clientConn1).
		Times(1)

	err = doExecute()
	s.Error(err)
	solErr = &serviceerrors.ShardOwnershipLost{}
	s.ErrorAs(err, &solErr)
	s.Empty(solErr.OwnerHost)
	s.Equal(3, opCalls)

	// opCall 3 & 4: return SOL with new owner hint.
	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr1)), nil).
		Times(1)

	s.connections.EXPECT().
		getOrCreateClientConn(testAddr1).
		Return(clientConn1).
		Times(1)
	s.connections.EXPECT().
		resetConnectBackoff(clientConn1).
		Times(1)

	clientConn2 := clientConnection{
		historyClient: mockClient2,
	}
	s.connections.EXPECT().
		getOrCreateClientConn(testAddr2).
		Return(clientConn2).
		Times(1)
	s.connections.EXPECT().
		resetConnectBackoff(clientConn2).
		Times(1)

	err = doExecute()
	s.NoError(err)
	s.Equal(5, opCalls)

	// OpCall 5: should use cached lookup & connection, so no additional mocks.
	err = doExecute()
	s.NoError(err)
}

func (s *cachingRedirectorSuite) TestClientForTargetByShard() {
	testAddr := rpcAddress("testaddr")
	shardID := int32(1)

	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr)), nil).
		Times(1)

	mockClient := historyservicemock.NewMockHistoryServiceClient(s.controller)
	clientConn := clientConnection{
		historyClient: mockClient,
	}
	s.connections.EXPECT().
		getOrCreateClientConn(testAddr).
		Return(clientConn)
	s.connections.EXPECT().
		resetConnectBackoff(clientConn).
		Times(1)

	r := newCachingRedirector(s.connections, s.resolver, s.logger)
	cli, err := r.clientForShardID(shardID)
	s.NoError(err)
	s.Equal(mockClient, cli)

	// No additional mocks; lookup should have been cached
	cli, err = r.clientForShardID(shardID)
	s.NoError(err)
	s.Equal(mockClient, cli)
}
