// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package client_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/quotas"
)

func TestRateLimitedPersistenceClients(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		err             error
		numRequests     int
		namespaceRPS    int
		systemRPS       int
		expectRateLimit bool
		expectedScope   enumspb.ResourceExhaustedScope
	}{
		{
			name:            "Namespace limit allow",
			err:             nil,
			numRequests:     10,
			namespaceRPS:    10,
			systemRPS:       100,
			expectRateLimit: false,
		},
		{
			name:            "Namespace limit hit",
			err:             nil,
			numRequests:     11,
			namespaceRPS:    10,
			systemRPS:       100,
			expectRateLimit: true,
			expectedScope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		},
		{
			name:            "System limit allow",
			err:             nil,
			numRequests:     10,
			namespaceRPS:    100,
			systemRPS:       10,
			expectRateLimit: false,
		},
		{
			name:            "System limit hit",
			err:             nil,
			numRequests:     11,
			namespaceRPS:    100,
			systemRPS:       10,
			expectRateLimit: true,
			expectedScope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctr := gomock.NewController(t)
			dataStoreFactory := mock.NewMockDataStoreFactory(ctr)

			shardStore := mock.NewMockShardStore(ctr)
			shardStore.EXPECT().AssertShardOwnership(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			dataStoreFactory.EXPECT().NewShardStore().AnyTimes().Return(shardStore, nil)

			queue := mock.NewMockQueue(ctr)
			queue.EXPECT().Init(gomock.Any(), gomock.Any()).Return(nil)
			queue.EXPECT().DeleteMessageFromDLQ(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			dataStoreFactory.EXPECT().NewQueue(gomock.Any()).AnyTimes().Return(queue, nil)

			executionStore := mock.NewMockExecutionStore(ctr)
			executionStore.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			dataStoreFactory.EXPECT().NewExecutionStore().AnyTimes().Return(executionStore, nil)

			metadataStore := mock.NewMockMetadataStore(ctr)
			metadataStore.EXPECT().DeleteNamespace(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			metadataStore.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
			dataStoreFactory.EXPECT().NewMetadataStore().AnyTimes().Return(metadataStore, nil)

			clusterMetadataStore := mock.NewMockClusterMetadataStore(ctr)
			clusterMetadataStore.EXPECT().DeleteClusterMetadata(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			clusterMetadataStore.EXPECT().GetClusterMetadata(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
			dataStoreFactory.EXPECT().NewClusterMetadataStore().AnyTimes().Return(clusterMetadataStore, nil)

			nexusStore := mock.NewMockNexusEndpointStore(ctr)
			nexusStore.EXPECT().DeleteNexusEndpoint(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			nexusStore.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
			dataStoreFactory.EXPECT().NewNexusEndpointStore().AnyTimes().Return(nexusStore, nil)

			taskStore := mock.NewMockTaskStore(ctr)
			taskStore.EXPECT().GetTasks(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
			dataStoreFactory.EXPECT().NewTaskStore().AnyTimes().Return(taskStore, nil)

			burstRatioFn := func() float64 {
				return 1.0
			}
			systemRequestRateLimiter := quotas.NewRequestRateLimiterAdapter(
				quotas.NewDefaultRateLimiter(
					func() float64 { return float64(tc.systemRPS) },
					burstRatioFn,
				),
			)
			namespaceRequestRateLimiter := quotas.NewRequestRateLimiterAdapter(
				quotas.NewDefaultRateLimiter(
					func() float64 { return float64(tc.namespaceRPS) },
					burstRatioFn,
				),
			)
			factory := client.NewFactory(
				dataStoreFactory,
				&config.Persistence{
					NumHistoryShards: 1,
				},
				systemRequestRateLimiter,
				namespaceRequestRateLimiter,
				serialization.NewSerializer(),
				nil,
				"",
				nil,
				nil,
				nil,
			)
			shardManager, _ := factory.NewShardManager()
			executionManager, _ := factory.NewExecutionManager()
			metadataManager, _ := factory.NewMetadataManager()
			clusterMetadataManager, _ := factory.NewClusterMetadataManager()
			nexusManager, _ := factory.NewNexusEndpointManager()
			namespaceQueue, _ := factory.NewNamespaceReplicationQueue()

			// Make calls to different manager objects to verify that RPS is enforced.
			persistenceCalls := []struct {
				name string
				call func() error
			}{
				{
					name: "AssertShardOwnership",
					call: func() error {
						return shardManager.AssertShardOwnership(context.Background(), &persistence.AssertShardOwnershipRequest{ShardID: 0})
					},
				},
				{
					name: "DeleteWorkflowExecution",
					call: func() error {
						return executionManager.DeleteWorkflowExecution(context.Background(), &persistence.DeleteWorkflowExecutionRequest{})
					},
				},
				{
					name: "DeleteNamespace",
					call: func() error {
						return metadataManager.DeleteNamespace(context.Background(), &persistence.DeleteNamespaceRequest{})
					},
				},
				{
					name: "DeleteClusterMetadata",
					call: func() error {
						return clusterMetadataManager.DeleteClusterMetadata(context.Background(), &persistence.DeleteClusterMetadataRequest{ClusterName: "test"})
					},
				},
				{
					name: "DeleteNexusEndpoint",
					call: func() error {
						return nexusManager.DeleteNexusEndpoint(context.Background(), &persistence.DeleteNexusEndpointRequest{})
					},
				},
				{
					name: "DeleteMessageFromDLQ",
					call: func() error {
						return namespaceQueue.DeleteMessageFromDLQ(context.Background(), 0)
					},
				},
			}
			var err error
			for _, persistenceCall := range persistenceCalls {
				t.Run(fmt.Sprintf("%s %s", tc.name, persistenceCall.name), func(t *testing.T) {
					// Generate load by sending a number of requests.
					for i := 0; i < tc.numRequests; i++ {
						err = persistenceCall.call()
						if err != nil {
							// Assert resource exhausted at last request.
							assert.Equal(t, tc.numRequests-1, i)
							break
						}
					}
					// Check if the rate limit is hit.
					if tc.expectRateLimit {
						assert.ErrorContains(t, err, fmt.Sprintf("%s Persistence Max QPS Reached", tc.expectedScope.String()))
						var resourceExhausted *serviceerror.ResourceExhausted
						errors.As(err, &resourceExhausted)
						assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, resourceExhausted.Cause)
						assert.Equal(t, tc.expectedScope, resourceExhausted.Scope)
					} else {
						assert.NoError(t, err)
					}
					// Sleep for 1 second for the rate limits to reset.
					time.Sleep(1 * time.Second)
				})

			}
		})
	}
}
