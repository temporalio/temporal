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

package build_ids

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"google.golang.org/grpc"
)

func Test_processUserDataEntry_AcceptsNilVersioningData(t *testing.T) {
	ctrl := gomock.NewController(t)
	timeSource := namespace.NewMockClock(ctrl)
	timeSource.EXPECT().Now().AnyTimes()

	a := &Activities{
		timeSource: timeSource,
	}

	ctx := context.Background()
	c0 := hlc.Zero(0)
	userData := &persistencespb.TaskQueueUserData{
		Clock:          &c0,
		VersioningData: nil,
	}

	buildIdsRemoved, err := a.processUserDataEntry(ctx, nil, heartbeatDetails{}, namespace.NewNamespaceForTest(nil, nil, false, nil, 0), &persistence.TaskQueueUserDataEntry{
		TaskQueue: "test",
		UserData: &persistencespb.VersionedTaskQueueUserData{
			Version: 0,
			Data:    userData,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string(nil), buildIdsRemoved)
	require.True(t, hlc.Equal(c0, *userData.Clock))
}

func Test_processUserDataEntry_PutsTombstonesOnEligableBuildIds(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	ctrl := gomock.NewController(t)
	visiblityManager := manager.NewMockVisibilityManager(ctrl)
	rateLimiter := quotas.NewMockRateLimiter(ctrl)
	timeSource := namespace.NewMockClock(ctrl)

	a := &Activities{
		logger:            log.NewCLILogger(),
		visibilityManager: visiblityManager,
		timeSource:        timeSource,
	}

	visiblityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Times(4).DoAndReturn(
		func(ctx context.Context, request *manager.CountWorkflowExecutionsRequest) (*manager.CountWorkflowExecutionsResponse, error) {
			count := 0
			fmt.Println(request.Query)
			if strings.Contains(request.Query, fmt.Sprintf("'%s'", common.VersionedBuildIdSearchAttribute("v3.0"))) {
				count = 1
			}
			return &manager.CountWorkflowExecutionsResponse{
				Count: int64(count),
			}, nil
		},
	)
	rateLimiter.EXPECT().Wait(gomock.Any()).Times(4)
	timeSource.EXPECT().Now().AnyTimes()

	heartbeatRecorded := false
	env.SetOnActivityHeartbeatListener(func(activityInfo *activity.Info, details converter.EncodedValues) {
		heartbeatRecorded = true
	})

	c0 := hlc.Zero(0)
	userData := &persistencespb.TaskQueueUserData{
		Clock: &c0,
		VersioningData: &persistencespb.VersioningData{
			VersionSets: []*persistencespb.CompatibleVersionSet{
				{
					SetIds: []string{"v1"},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                   "v1.0",
							State:                persistencespb.STATE_DELETED,
							StateUpdateTimestamp: &c0,
						},
						{
							Id:                   "v1.1",
							State:                persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp: &c0,
						},
						{
							Id:                   "v1.2",
							State:                persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp: &c0,
						},
					},
					DefaultUpdateTimestamp: &c0,
				},
				{
					SetIds: []string{"v2"},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                   "v2.0",
							State:                persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp: &c0,
						},
					},
					DefaultUpdateTimestamp: &c0,
				},
				{
					SetIds: []string{"v3"},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                   "v3.0",
							State:                persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp: &c0,
						},
						{
							Id:                   "v3.1",
							State:                persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp: &c0,
						},
					},
					DefaultUpdateTimestamp: &c0,
				},
				{
					SetIds: []string{"v4"},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                   "v4.0",
							State:                persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp: &c0,
						},
					},
					DefaultUpdateTimestamp: &c0,
				},
			},
			DefaultUpdateTimestamp: &c0,
		},
	}

	act := func(ctx context.Context) ([]string, error) {
		return a.processUserDataEntry(ctx, rateLimiter, heartbeatDetails{}, namespace.NewNamespaceForTest(nil, nil, false, nil, 0), &persistence.TaskQueueUserDataEntry{
			TaskQueue: "test",
			UserData: &persistencespb.VersionedTaskQueueUserData{
				Version: 0,
				Data:    userData,
			},
		})
	}
	env.RegisterActivity(act)
	removedBuildIDsEncoded, err := env.ExecuteActivity(act)
	require.NoError(t, err)
	var removedBuildIDs []string
	err = removedBuildIDsEncoded.Get(&removedBuildIDs)
	require.NoError(t, err)
	require.Equal(t, []string{"v1.1", "v1.2", "v2.0"}, removedBuildIDs)
	c1 := c0
	c1.Version++

	expected := &persistencespb.TaskQueueUserData{
		Clock: &c1,
		VersioningData: &persistencespb.VersioningData{
			VersionSets: []*persistencespb.CompatibleVersionSet{
				{
					SetIds: []string{"v1"},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                   "v1.0",
							State:                persistencespb.STATE_DELETED,
							StateUpdateTimestamp: &c0,
						},
						{
							Id:                   "v1.1",
							State:                persistencespb.STATE_DELETED,
							StateUpdateTimestamp: &c1,
						},
						{
							Id:                   "v1.2",
							State:                persistencespb.STATE_DELETED,
							StateUpdateTimestamp: &c1,
						},
					},
					DefaultUpdateTimestamp: &c0,
				},
				{
					SetIds: []string{"v2"},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                   "v2.0",
							State:                persistencespb.STATE_DELETED,
							StateUpdateTimestamp: &c1,
						},
					},
					DefaultUpdateTimestamp: &c0,
				},
				{
					SetIds: []string{"v3"},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                   "v3.0",
							State:                persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp: &c0,
						},
						{
							Id:                   "v3.1",
							State:                persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp: &c0,
						},
					},
					DefaultUpdateTimestamp: &c0,
				},
				{
					SetIds: []string{"v4"},
					BuildIds: []*persistencespb.BuildId{
						{
							Id:                   "v4.0",
							State:                persistencespb.STATE_ACTIVE,
							StateUpdateTimestamp: &c0,
						},
					},
					DefaultUpdateTimestamp: &c0,
				},
			},
			DefaultUpdateTimestamp: &c0,
		},
	}

	require.Equal(t, expected, userData)
	require.True(t, heartbeatRecorded)
}

func Test_clearTombstones(t *testing.T) {
	c0 := hlc.Zero(0)
	data := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds: []string{"v1"},
				BuildIds: []*persistencespb.BuildId{
					{
						Id:                   "v1.0",
						State:                persistencespb.STATE_DELETED,
						StateUpdateTimestamp: &c0,
					},
					{
						Id:                   "v1.1",
						State:                persistencespb.STATE_DELETED,
						StateUpdateTimestamp: &c0,
					},
				},
				DefaultUpdateTimestamp: &c0,
			},
			{
				SetIds: []string{"v2"},
				BuildIds: []*persistencespb.BuildId{
					{
						Id:                   "v2.0",
						State:                persistencespb.STATE_DELETED,
						StateUpdateTimestamp: &c0,
					},
				},
				DefaultUpdateTimestamp: &c0,
			},
			{
				SetIds: []string{"v3"},
				BuildIds: []*persistencespb.BuildId{
					{
						Id:                   "v3.0",
						State:                persistencespb.STATE_DELETED,
						StateUpdateTimestamp: &c0,
					},
					{
						Id:                   "v3.1",
						State:                persistencespb.STATE_ACTIVE,
						StateUpdateTimestamp: &c0,
					},
				},
				DefaultUpdateTimestamp: &c0,
			},
		},
		DefaultUpdateTimestamp: &c0,
	}
	clearTombstones(data)

	expected := &persistencespb.VersioningData{
		VersionSets: []*persistencespb.CompatibleVersionSet{
			{
				SetIds: []string{"v3"},
				BuildIds: []*persistencespb.BuildId{
					{
						Id:                   "v3.1",
						State:                persistencespb.STATE_ACTIVE,
						StateUpdateTimestamp: &c0,
					},
				},
				DefaultUpdateTimestamp: &c0,
			},
		},
		DefaultUpdateTimestamp: &c0,
	}
	require.Equal(t, expected, data)
}

func Test_ScavengeBuildIds_Heartbeats(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	iceptor := heartbeatRecordingInterceptor{T: t}
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{&iceptor}})

	ctrl := gomock.NewController(t)
	visiblityManager := manager.NewMockVisibilityManager(ctrl)
	rateLimiter := quotas.NewMockRateLimiter(ctrl)
	timeSource := namespace.NewMockClock(ctrl)
	metadataManager := persistence.NewMockMetadataManager(ctrl)
	taskManager := persistence.NewMockTaskManager(ctrl)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	matchingClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)

	a := &Activities{
		logger:            log.NewCLILogger(),
		visibilityManager: visiblityManager,
		timeSource:        timeSource,
		metadataManager:   metadataManager,
		taskManager:       taskManager,
		namespaceRegistry: namespaceRegistry,
		matchingClient:    matchingClient,
	}

	rateLimiter.EXPECT().Wait(gomock.Any()).AnyTimes()
	timeSource.EXPECT().Now().AnyTimes()
	visiblityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).AnyTimes().Return(&manager.CountWorkflowExecutionsResponse{
		Count: 0,
	}, nil)

	c0 := hlc.Zero(0)
	c1 := c0
	c1.Version++

	initialHeartbeat := heartbeatDetails{
		NamespaceIdx:           1,
		TaskQueueIdx:           1,
		NamespaceNextPageToken: []byte{0xde, 0xad},
		TaskQueueNextPageToken: []byte{0xbe, 0xef},
	}
	metadataManager.EXPECT().ListNamespaces(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, request *persistence.ListNamespacesRequest) (*persistence.ListNamespacesResponse, error) {
			require.Equal(t, initialHeartbeat.NamespaceNextPageToken, request.NextPageToken)
			return &persistence.ListNamespacesResponse{
				Namespaces: []*persistence.GetNamespaceResponse{
					{
						// skip
					},
					{
						Namespace: &persistencespb.NamespaceDetail{
							Info: &persistencespb.NamespaceInfo{
								Id:   "local",
								Name: "local",
							},
						},
					},
					{
						Namespace: &persistencespb.NamespaceDetail{
							Info: &persistencespb.NamespaceInfo{
								Id:   "global",
								Name: "global",
							},
						},
					},
				},
				NextPageToken: []byte{},
			}, nil
		},
	)
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).AnyTimes().DoAndReturn(func(id namespace.ID) (*namespace.Namespace, error) {
		global := false
		if id == namespace.ID("global") {
			global = true
		}
		return namespace.NewNamespaceForTest(nil, nil, global, nil, 0), nil
	})

	numTaskQueueListCalls := 0
	// Called twice, once for local namespace and once for global namespace
	taskManager.EXPECT().ListTaskQueueUserDataEntries(gomock.Any(), gomock.Any()).Times(2).DoAndReturn(
		func(ctx context.Context, request *persistence.ListTaskQueueUserDataEntriesRequest) (*persistence.ListTaskQueueUserDataEntriesResponse, error) {
			numTaskQueueListCalls++
			if numTaskQueueListCalls == 1 {
				require.Equal(t, initialHeartbeat.TaskQueueNextPageToken, request.NextPageToken)
			}
			return &persistence.ListTaskQueueUserDataEntriesResponse{
				Entries: []*persistence.TaskQueueUserDataEntry{
					{
						// Nothing to do here (skipped for local namespace)
						TaskQueue: "without-data",
						UserData: &persistencespb.VersionedTaskQueueUserData{
							Version: 1,
							Data: &persistencespb.TaskQueueUserData{
								Clock: &c0,
							},
						},
					},
					{
						// v1.0 should be deleted
						TaskQueue: "with-data",
						UserData: &persistencespb.VersionedTaskQueueUserData{
							Version: 1,
							Data: &persistencespb.TaskQueueUserData{
								Clock: &c0,
								VersioningData: &persistencespb.VersioningData{
									VersionSets: []*persistencespb.CompatibleVersionSet{
										{
											SetIds: []string{"v1"},
											BuildIds: []*persistencespb.BuildId{
												{
													Id:                   "v1.0",
													State:                persistencespb.STATE_ACTIVE,
													StateUpdateTimestamp: &c0,
												},
												{
													Id:                   "v1.1",
													State:                persistencespb.STATE_ACTIVE,
													StateUpdateTimestamp: &c0,
												},
											},
											DefaultUpdateTimestamp: &c0,
										},
									},
								},
							},
						},
					},
				},
				NextPageToken: []byte{},
			}, nil
		},
	)
	numUpdateCalls := 0
	matchingClient.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any()).Times(3).DoAndReturn(
		func(ctx context.Context, in *matchingservice.UpdateTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {
			numUpdateCalls++
			require.Equal(t, "with-data", in.TaskQueue)
			require.Equal(t, []string{"v1.0"}, in.BuildIdsRemoved)
			if numUpdateCalls == 1 {
				require.Equal(t, "local", in.NamespaceId)
				require.Equal(t, &persistencespb.VersionedTaskQueueUserData{
					Version: 2,
					Data: &persistencespb.TaskQueueUserData{
						Clock: &c1,
						VersioningData: &persistencespb.VersioningData{
							VersionSets: []*persistencespb.CompatibleVersionSet{
								{
									SetIds: []string{"v1"},
									BuildIds: []*persistencespb.BuildId{
										{
											Id:                   "v1.1",
											State:                persistencespb.STATE_ACTIVE,
											StateUpdateTimestamp: &c0,
										},
									},
									DefaultUpdateTimestamp: &c0,
								},
							},
						},
					},
				}, in.UserData)
			} else if numUpdateCalls == 2 {
				require.Equal(t, "global", in.NamespaceId)
				require.Equal(t, &persistencespb.VersionedTaskQueueUserData{
					Version: 2,
					Data: &persistencespb.TaskQueueUserData{
						Clock: &c1,
						VersioningData: &persistencespb.VersioningData{
							VersionSets: []*persistencespb.CompatibleVersionSet{
								{
									SetIds: []string{"v1"},
									BuildIds: []*persistencespb.BuildId{
										{
											Id:                   "v1.0",
											State:                persistencespb.STATE_DELETED,
											StateUpdateTimestamp: &c1,
										},
										{
											Id:                   "v1.1",
											State:                persistencespb.STATE_ACTIVE,
											StateUpdateTimestamp: &c0,
										},
									},
									DefaultUpdateTimestamp: &c0,
								},
							},
						},
					},
				}, in.UserData)
			} else {
				require.Equal(t, "global", in.NamespaceId)
				require.Equal(t, &persistencespb.VersionedTaskQueueUserData{
					Version: 3,
					Data: &persistencespb.TaskQueueUserData{
						Clock: &c1,
						VersioningData: &persistencespb.VersioningData{
							VersionSets: []*persistencespb.CompatibleVersionSet{
								{
									SetIds: []string{"v1"},
									BuildIds: []*persistencespb.BuildId{
										{
											Id:                   "v1.1",
											State:                persistencespb.STATE_ACTIVE,
											StateUpdateTimestamp: &c0,
										},
									},
									DefaultUpdateTimestamp: &c0,
								},
							},
						},
					},
				}, in.UserData)
			}

			return &matchingservice.UpdateTaskQueueUserDataResponse{}, nil
		},
	)
	matchingClient.EXPECT().ReplicateTaskQueueUserData(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, in *matchingservice.ReplicateTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.ReplicateTaskQueueUserDataResponse, error) {
			require.Equal(t, "global", in.NamespaceId)
			require.Equal(t, "with-data", in.TaskQueue)
			require.Equal(t, &persistencespb.TaskQueueUserData{
				Clock: &c1,
				VersioningData: &persistencespb.VersioningData{
					VersionSets: []*persistencespb.CompatibleVersionSet{
						{
							SetIds: []string{"v1"},
							BuildIds: []*persistencespb.BuildId{
								{
									Id:                   "v1.0",
									State:                persistencespb.STATE_DELETED,
									StateUpdateTimestamp: &c1,
								},
								{
									Id:                   "v1.1",
									State:                persistencespb.STATE_ACTIVE,
									StateUpdateTimestamp: &c0,
								},
							},
							DefaultUpdateTimestamp: &c0,
						},
					},
				},
			}, in.UserData)

			return &matchingservice.ReplicateTaskQueueUserDataResponse{}, nil
		},
	)
	env.SetHeartbeatDetails(initialHeartbeat)
	env.RegisterActivity(a)
	_, err := env.ExecuteActivity(a.ScavengeBuildIds, BuildIdScavangerInput{})
	require.NoError(t, err)
	require.Equal(t, []heartbeatDetails{
		{
			NamespaceIdx:           1,
			TaskQueueIdx:           1,
			NamespaceNextPageToken: initialHeartbeat.NamespaceNextPageToken,
			TaskQueueNextPageToken: initialHeartbeat.TaskQueueNextPageToken,
		},
		{
			NamespaceIdx:           1,
			TaskQueueIdx:           2,
			NamespaceNextPageToken: initialHeartbeat.NamespaceNextPageToken,
			TaskQueueNextPageToken: initialHeartbeat.TaskQueueNextPageToken,
		},
		{
			NamespaceIdx:           2,
			TaskQueueIdx:           0,
			NamespaceNextPageToken: initialHeartbeat.NamespaceNextPageToken,
			TaskQueueNextPageToken: []byte{},
		},
		{
			NamespaceIdx:           2,
			TaskQueueIdx:           1,
			NamespaceNextPageToken: initialHeartbeat.NamespaceNextPageToken,
			TaskQueueNextPageToken: []byte{},
		},
		{
			// Another heartbeat while counting
			NamespaceIdx:           2,
			TaskQueueIdx:           1,
			NamespaceNextPageToken: initialHeartbeat.NamespaceNextPageToken,
			TaskQueueNextPageToken: []byte{},
		},
		{
			NamespaceIdx:           2,
			TaskQueueIdx:           2,
			NamespaceNextPageToken: initialHeartbeat.NamespaceNextPageToken,
			TaskQueueNextPageToken: []byte{},
		},
		{
			NamespaceIdx:           3,
			TaskQueueIdx:           0,
			NamespaceNextPageToken: initialHeartbeat.NamespaceNextPageToken,
			TaskQueueNextPageToken: []byte{},
		},
	}, iceptor.recordedHeartbeats)
}

// The SDK's test environment throttles emitted heartbeat forcing us to use an interceptor to record the heartbeat details
type heartbeatRecordingInterceptor struct {
	interceptor.WorkerInterceptorBase
	interceptor.ActivityInboundInterceptorBase
	interceptor.ActivityOutboundInterceptorBase
	recordedHeartbeats []heartbeatDetails
	T                  *testing.T
}

func (i *heartbeatRecordingInterceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	i.ActivityInboundInterceptorBase.Next = next
	return i
}

func (i *heartbeatRecordingInterceptor) Init(outbound interceptor.ActivityOutboundInterceptor) error {
	i.ActivityOutboundInterceptorBase.Next = outbound
	return i.ActivityInboundInterceptorBase.Init(i)
}

func (i *heartbeatRecordingInterceptor) RecordHeartbeat(ctx context.Context, details ...interface{}) {
	d, ok := details[0].(heartbeatDetails)
	require.True(i.T, ok, "invalid heartbeat details")
	i.recordedHeartbeats = append(i.recordedHeartbeats, d)
	i.ActivityOutboundInterceptorBase.Next.RecordHeartbeat(ctx, details...)
}
