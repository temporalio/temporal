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

package history_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/internal/nettest"
	"google.golang.org/grpc"
)

type (
	testHistoryService struct {
		historyservice.UnimplementedHistoryServiceServer
	}
	testRPCFactory struct {
		base            *nettest.RPCFactory
		dialedAddresses []string
	}
)

func TestErrLookup(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	serviceResolver := membership.NewMockServiceResolver(ctrl)
	serviceResolver.EXPECT().Lookup("1").Return(nil, assert.AnError).AnyTimes()
	client := history.NewClient(
		dynamicconfig.NewNoopCollection(),
		serviceResolver,
		log.NewTestLogger(),
		1,
		nil,
		time.Duration(0),
	)

	for _, tc := range []struct {
		name string
		fn   func() error
	}{
		{
			name: "GetDLQTasks",
			fn: func() error {
				_, err := client.GetDLQTasks(context.Background(), &historyservice.GetDLQTasksRequest{})
				return err
			},
		},
		{
			name: "DeleteDLQTasks",
			fn: func() error {
				_, err := client.DeleteDLQTasks(context.Background(), &historyservice.DeleteDLQTasksRequest{})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.fn()
			var unavailableErr *serviceerror.Unavailable
			require.ErrorAs(t, err, &unavailableErr, "Should return an 'Unavailable' error when there "+
				"are no history hosts available to serve the request")
			assert.ErrorContains(t, err, assert.AnError.Error())
		})
	}
}

// This tests our strategy for getting history hosts to serve shard-agnostic requests, like those interacting with the
// DLQ. For such requests, we should round-robin over all available history shards. In addition, we should re-use any
// available connections when the round-robin wraps around.
func TestShardAgnosticConnectionStrategy(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		fn   func(client historyservice.HistoryServiceClient) error
	}{
		{
			name: "GetDLQTasks",
			fn: func(client historyservice.HistoryServiceClient) error {
				_, err := client.GetDLQTasks(context.Background(), &historyservice.GetDLQTasksRequest{})
				return err
			},
		},
		{
			name: "DeleteDLQTasks",
			fn: func(client historyservice.HistoryServiceClient) error {
				_, err := client.DeleteDLQTasks(context.Background(), &historyservice.DeleteDLQTasksRequest{})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			// Create a service resolver that just returns 2 hosts for the first 3 requests. We want to send 3 requests
			// with 2 hosts so that we can verify that we re-use the connection of "test1" on the last request.
			serviceResolver := membership.NewMockServiceResolver(ctrl)
			serviceResolver.EXPECT().Lookup("1").Return(membership.NewHostInfoFromAddress("test1"), nil)
			serviceResolver.EXPECT().Lookup("2").Return(membership.NewHostInfoFromAddress("test2"), nil)
			serviceResolver.EXPECT().Lookup("1").Return(membership.NewHostInfoFromAddress("test1"), nil)

			// Create an in-memory gRPC server.
			listener := nettest.NewListener(nettest.NewPipe())
			rpcFactory := &testRPCFactory{
				base: nettest.NewRPCFactory(listener),
			}
			grpcServer := grpc.NewServer()
			testSvc := &testHistoryService{}
			historyservice.RegisterHistoryServiceServer(grpcServer, testSvc)
			errs := make(chan error)
			go func() {
				errs <- grpcServer.Serve(listener)
			}()
			defer func() {
				grpcServer.Stop()
				require.NoError(t, <-errs)
			}()

			// Send 3 requests to verify that we re-use a connection for the last request.
			client := history.NewClient(
				dynamicconfig.NewNoopCollection(),
				serviceResolver,
				log.NewTestLogger(),
				2,
				rpcFactory,
				time.Duration(0),
			)
			for i := 0; i < 3; i++ {
				err := tc.fn(client)
				require.NoError(t, err)
			}

			// Verify that there are no repeated dialed addresses (indicating that we re-used the connection).
			assert.Equal(
				t,
				[]string{"test1", "test2"},
				rpcFactory.dialedAddresses,
				"Should cache the client connection and reuse it for subsequent requests",
			)
		})
	}
}

func (s *testHistoryService) GetDLQTasks(
	context.Context,
	*historyservice.GetDLQTasksRequest,
) (*historyservice.GetDLQTasksResponse, error) {
	return &historyservice.GetDLQTasksResponse{}, nil
}

func (s *testHistoryService) DeleteDLQTasks(
	context.Context,
	*historyservice.DeleteDLQTasksRequest,
) (*historyservice.DeleteDLQTasksResponse, error) {
	return &historyservice.DeleteDLQTasksResponse{}, nil
}

func (t *testRPCFactory) CreateInternodeGRPCConnection(rpcAddress string) *grpc.ClientConn {
	t.dialedAddresses = append(t.dialedAddresses, rpcAddress)
	return t.base.CreateInternodeGRPCConnection(rpcAddress)
}
