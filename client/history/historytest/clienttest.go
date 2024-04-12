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

// Package historytest contains library test functions for [history.NewClient] that use ahistory task queue manager.
// These are not test functions themselves because we construct database clients in another package, which will in turn
// call this function, but we don't want to put the testing logic there because it's not specific to any database, but
// it is specific to the [history] package.
package historytest

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
	"google.golang.org/grpc"

	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/internal/nettest"
	historyserver "go.temporal.io/server/service/history"
	"go.temporal.io/server/service/history/tasks"
)

// fakeTracerProvider is needed to construct a [historyserver.Handler] object.
type fakeTracerProvider struct {
	embedded.TracerProvider
}

func (f fakeTracerProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return nil
}

var _ trace.TracerProvider = (*fakeTracerProvider)(nil)

// TestClient works by doing the following:
//  1. Enqueue some tasks
//  2. Start a server which serves the DLQ endpoints
//  3. Create a client which connects to the server
//  4. Use the client to read the tasks
func TestClient(t *testing.T, historyTaskQueueManager persistence.HistoryTaskQueueManager) {
	ctrl := gomock.NewController(t)

	listener := nettest.NewListener(nettest.NewPipe())

	serveErrs := make(chan error, 1)
	grpcServer := createServer(historyTaskQueueManager)
	go func() {
		serveErrs <- grpcServer.Serve(listener)
	}()

	client := createClient(ctrl, listener)

	t.Run("ReadDLQTasks", func(t *testing.T) {
		t.Parallel()
		queueKey := persistencetest.GetQueueKey(t, persistencetest.WithQueueType(persistence.QueueTypeHistoryDLQ))
		numTasks := 2
		_, err := historyTaskQueueManager.CreateQueue(context.Background(), &persistence.CreateQueueRequest{
			QueueKey: queueKey,
		})
		require.NoError(t, err)
		enqueueTasks(t, historyTaskQueueManager, numTasks, queueKey.SourceCluster, queueKey.TargetCluster)
		readTasks(t, numTasks, client, queueKey.SourceCluster, queueKey.TargetCluster)
	})
	t.Run("DeleteDLQTasks", func(t *testing.T) {
		t.Parallel()
		queueKey := persistencetest.GetQueueKey(t, persistencetest.WithQueueType(persistence.QueueTypeHistoryDLQ))
		_, err := historyTaskQueueManager.CreateQueue(context.Background(), &persistence.CreateQueueRequest{
			QueueKey: queueKey,
		})
		require.NoError(t, err)
		enqueueTasks(t, historyTaskQueueManager, 2, queueKey.SourceCluster, queueKey.TargetCluster)
		dlqKey := &commonspb.HistoryDLQKey{
			TaskCategory:  int32(tasks.CategoryTransfer.ID()),
			SourceCluster: queueKey.SourceCluster,
			TargetCluster: queueKey.TargetCluster,
		}
		_, err = client.DeleteDLQTasks(context.Background(), &historyservice.DeleteDLQTasksRequest{
			DlqKey: dlqKey,
			InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
				MessageId: persistence.FirstQueueMessageID,
			},
		})
		require.NoError(t, err)
		res, err := client.GetDLQTasks(context.Background(), &historyservice.GetDLQTasksRequest{
			DlqKey:   dlqKey,
			PageSize: 10,
		})
		require.NoError(t, err)
		assert.Equal(t, 1, len(res.DlqTasks))
		assert.Equal(t, int64(persistence.FirstQueueMessageID+1), res.DlqTasks[0].Metadata.MessageId)
	})

	t.Cleanup(func() {
		grpcServer.GracefulStop()
		assert.NoError(t, <-serveErrs)
	})
}

func readTasks(
	t *testing.T,
	numTasks int,
	client historyservice.HistoryServiceClient,
	sourceCluster string,
	targetCluster string,
) {
	t.Helper()

	var nextPageToken []byte

	// We want to run a test where the client makes multiple requests to the server because the client is stateful. In
	// particular, the first request here should establish a connection, and the next one should reuse that connection.
	for i := 0; i < numTasks; i++ {
		res, err := client.GetDLQTasks(context.Background(), &historyservice.GetDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory:  int32(tasks.CategoryTransfer.ID()),
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
			},
			PageSize:      1,
			NextPageToken: nextPageToken,
		})
		require.NoError(t, err)
		assert.Equal(t, 1, len(res.DlqTasks))
		assert.Equal(t, int64(persistence.FirstQueueMessageID+i), res.DlqTasks[0].Metadata.MessageId)
		nextPageToken = res.NextPageToken
	}
}

func createServer(historyTaskQueueManager persistence.HistoryTaskQueueManager) *grpc.Server {
	// TODO: find a better way to create a history handler
	historyHandler := historyserver.HandlerProvider(historyserver.NewHandlerArgs{
		TaskQueueManager:     historyTaskQueueManager,
		TracerProvider:       fakeTracerProvider{},
		TaskCategoryRegistry: tasks.NewDefaultTaskCategoryRegistry(),
	})
	grpcServer := grpc.NewServer()
	historyservice.RegisterHistoryServiceServer(grpcServer, historyHandler)
	return grpcServer
}

func createClient(ctrl *gomock.Controller, listener *nettest.PipeListener) historyservice.HistoryServiceClient {
	serviceResolver := membership.NewMockServiceResolver(ctrl)
	address := membership.NewHostInfoFromAddress("127.0.0.1:7104")
	serviceResolver.EXPECT().Members().Return([]membership.HostInfo{
		address,
	}).AnyTimes()
	serviceResolver.EXPECT().Lookup(gomock.Any()).Return(address, nil).AnyTimes()
	rpcFactory := nettest.NewRPCFactory(listener)
	client := history.NewClient(
		dynamicconfig.NewNoopCollection(),
		serviceResolver,
		log.NewTestLogger(),
		1,
		rpcFactory,
		time.Duration(0),
	)
	return client
}

func enqueueTasks(
	t *testing.T,
	historyTaskQueueManager persistence.HistoryTaskQueueManager,
	numTasks int,
	sourceCluster string,
	targetCluster string,
) {
	t.Helper()

	task := &tasks.WorkflowTask{
		TaskID: 42,
	}
	for i := 0; i < numTasks; i++ {
		_, err := historyTaskQueueManager.EnqueueTask(context.Background(), &persistence.EnqueueTaskRequest{
			QueueType:     persistence.QueueTypeHistoryDLQ,
			SourceCluster: sourceCluster,
			TargetCluster: targetCluster,
			Task:          task,
			SourceShardID: 1,
		})
		require.NoError(t, err)
	}
}
