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
	"go.temporal.io/server/common/membership"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	basicRedirectorSuite struct {
		suite.Suite
		*require.Assertions

		controller  *gomock.Controller
		connections *MockconnectionPool
		resolver    *membership.MockServiceResolver
	}
)

func TestBasicRedirectorSuite(t *testing.T) {
	s := new(basicRedirectorSuite)
	suite.Run(t, s)
}

func (s *basicRedirectorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.connections = NewMockconnectionPool(s.controller)
	s.resolver = membership.NewMockServiceResolver(s.controller)
}

func (s *basicRedirectorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *basicRedirectorSuite) TestShardCheck() {
	r := newBasicRedirector(s.connections, s.resolver)

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

func opErrorTest(s *basicRedirectorSuite, clientOp clientOperation, verify func(err error)) {
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

	r := newBasicRedirector(s.connections, s.resolver)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := r.execute(ctx, shardID, clientOp)
	verify(err)
}

func (s *basicRedirectorSuite) TestDeadlineExceededError() {
	opErrorTest(s,
		func(ctx context.Context, client historyservice.HistoryServiceClient) error {
			<-ctx.Done()
			return ctx.Err()
		},
		func(err error) {
			s.ErrorIs(err, context.DeadlineExceeded)
		})
}

func (s *basicRedirectorSuite) TestUnavailableError() {
	opErrorTest(s,
		func(ctx context.Context, client historyservice.HistoryServiceClient) error {
			return serviceerror.NewUnavailable("unavail")
		},
		func(err error) {
			unavil := &serviceerror.Unavailable{}
			s.ErrorAs(err, &unavil)
		})
}

func (s *basicRedirectorSuite) TestShardOwnershipLostErrors() {
	testAddr1 := rpcAddress("testaddr1")
	testAddr2 := rpcAddress("testaddr2")
	shardID := int32(1)

	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr1)), nil).
		Times(2)

	mockClient1 := historyservicemock.NewMockHistoryServiceClient(s.controller)
	mockClient2 := historyservicemock.NewMockHistoryServiceClient(s.controller)
	clientConn1 := clientConnection{
		historyClient: mockClient1,
	}
	clientConn2 := clientConnection{
		historyClient: mockClient2,
	}
	s.connections.EXPECT().
		getOrCreateClientConn(testAddr1).
		Return(clientConn1).
		Times(2)

	r := newBasicRedirector(s.connections, s.resolver)
	attempt := 1
	doExecute := func() error {
		return r.execute(
			context.Background(),
			shardID,
			func(ctx context.Context, client historyservice.HistoryServiceClient) error {
				switch attempt {
				case 1:
					if client != mockClient1 {
						return errors.New("wrong client")
					}
					attempt++
					return serviceerrors.NewShardOwnershipLost("", "current")
				case 2:
					if client != mockClient1 {
						return errors.New("wrong client")
					}
					attempt++
					s.connections.EXPECT().
						getOrCreateClientConn(testAddr2).
						Return(clientConn2).
						Times(1)
					return serviceerrors.NewShardOwnershipLost(string(testAddr2), "current")
				case 3:
					if client != mockClient2 {
						return errors.New("wrong client")
					}
					attempt++
					return nil
				}
				return errors.New("too many attempts")
			},
		)
	}
	err := doExecute()
	s.Error(err)
	solErr := &serviceerrors.ShardOwnershipLost{}
	s.ErrorAs(err, &solErr)

	err = doExecute()
	s.NoError(err)
	s.Equal(4, attempt)
}

func (s *basicRedirectorSuite) TestClientForTargetByShard() {
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

	r := newBasicRedirector(s.connections, s.resolver)
	cli, err := r.clientForShardID(shardID)
	s.NoError(err)
	s.Equal(mockClient, cli)
}
