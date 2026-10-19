package matching

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	enhancedDescribeTestNamespaceID = "test-namespace-id"
	enhancedDescribeTestNamespace   = "test-namespace"
	enhancedDescribeTestTaskQueue   = "test-queue"
	enhancedDescribeTestPartitions  = 2
	enhancedDescribeTestCacheTTL    = 5 * time.Second
)

var enhancedDescribeBothTypes = []enumspb.TaskQueueType{
	enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	enumspb.TASK_QUEUE_TYPE_ACTIVITY,
}

type enhancedDescribeFlags struct {
	pollers, stats, reachability bool
}

func (f enhancedDescribeFlags) String() string {
	return fmt.Sprintf("pollers=%t,stats=%t,reachability=%t", f.pollers, f.stats, f.reachability)
}

// enhancedDescribeFixture runs the enhanced DescribeTaskQueue handler against a root partition
// with a real TTL cache and fake partitions that report only what a request asks them for.
type enhancedDescribeFixture struct {
	engine     *matchingEngineImpl
	timeSource *clock.EventTimeSource
	// generation changes what the fake partitions report, to tell fresh from cached data.
	generation     int
	partitionCalls int
	// cachePuts pairs every value handed to the cache with a copy taken at that moment.
	cachePuts [][2]proto.Message
}

func newEnhancedDescribeFixture(t *testing.T) *enhancedDescribeFixture {
	ctrl := gomock.NewController(t)
	f := &enhancedDescribeFixture{timeSource: clock.NewEventTimeSource()}

	config := defaultTestConfig()
	config.NumTaskqueueWritePartitions = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(enhancedDescribeTestPartitions)
	config.NumTaskqueueReadPartitions = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(enhancedDescribeTestPartitions)

	rootCache := cache.New(10000, &cache.Options{TTL: enhancedDescribeTestCacheTTL, TimeSource: f.timeSource})
	userData := NewMockuserDataManager(ctrl)
	userData.EXPECT().GetUserData().Return(nil, nil, nil).AnyTimes()
	rootPM := NewMocktaskQueuePartitionManager(ctrl)
	rootPM.EXPECT().WaitUntilInitialized(gomock.Any()).Return(nil).AnyTimes()
	rootPM.EXPECT().GetUserDataManager().Return(userData).AnyTimes()
	rootPM.EXPECT().GetCache(gomock.Any()).DoAndReturn(rootCache.Get).AnyTimes()
	rootPM.EXPECT().PutCache(gomock.Any(), gomock.Any()).Do(func(key, value any) {
		msg, ok := value.(proto.Message)
		require.True(t, ok)
		f.cachePuts = append(f.cachePuts, [2]proto.Message{msg, common.CloneProto(msg)})
		rootCache.Put(key, value)
	}).AnyTimes()

	rawClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	rawClient.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).DoAndReturn(f.describePartition).AnyTimes()

	rootPartition, err := tqid.PartitionFromProto(
		&taskqueuepb.TaskQueue{Name: enhancedDescribeTestTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		enhancedDescribeTestNamespaceID,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	)
	require.NoError(t, err)
	f.engine = &matchingEngineImpl{
		config:            config,
		matchingRawClient: rawClient,
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewNoopLogger(),
		partitions:        map[tqid.PartitionKey]taskQueuePartitionManager{rootPartition.Key(): rootPM},
	}
	return f
}

func (f *enhancedDescribeFixture) pollers(buildID string, taskQueueType enumspb.TaskQueueType, partitionIDs ...int32) []*taskqueuepb.PollerInfo {
	// One poller is seen by every partition and must be reported once.
	pollers := []*taskqueuepb.PollerInfo{
		{Identity: fmt.Sprintf("worker-%q-%s-gen%d", buildID, taskQueueType, f.generation)},
	}
	for _, partitionID := range partitionIDs {
		pollers = append(pollers, &taskqueuepb.PollerInfo{
			Identity: fmt.Sprintf("worker-%q-%s-gen%d-p%d", buildID, taskQueueType, f.generation, partitionID),
		})
	}
	return pollers
}

func (f *enhancedDescribeFixture) backlog(buildID string, taskQueueType enumspb.TaskQueueType, partitionID int32) int64 {
	return int64(1000*f.generation+100*len(buildID)+10*int(taskQueueType)) + int64(partitionID) + 1
}

func (f *enhancedDescribeFixture) describePartition(
	_ context.Context,
	request *matchingservice.DescribeTaskQueuePartitionRequest,
	_ ...grpc.CallOption,
) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
	f.partitionCalls++
	taskQueueType := request.GetTaskQueuePartition().GetTaskQueueType()
	partitionID := request.GetTaskQueuePartition().GetNormalPartitionId()
	versionsInfo := make(map[string]*taskqueuespb.TaskQueueVersionInfoInternal)
	for _, buildID := range request.GetVersions().GetBuildIds() {
		info := &taskqueuespb.PhysicalTaskQueueInfo{}
		if request.GetReportPollers() {
			info.Pollers = f.pollers(buildID, taskQueueType, partitionID)
		}
		if request.GetReportStats() {
			info.TaskQueueStats = &taskqueuepb.TaskQueueStats{
				ApproximateBacklogCount: f.backlog(buildID, taskQueueType, partitionID),
			}
		}
		versionsInfo[buildID] = &taskqueuespb.TaskQueueVersionInfoInternal{PhysicalTaskQueueInfo: info}
	}
	return &matchingservice.DescribeTaskQueuePartitionResponse{VersionsInfoInternal: versionsInfo}, nil
}

// describe requests the given build IDs, or the default (unversioned) build ID if there are none.
func (f *enhancedDescribeFixture) describe(
	t *testing.T,
	buildIDs []string,
	taskQueueTypes []enumspb.TaskQueueType,
	flags enhancedDescribeFlags,
) *matchingservice.DescribeTaskQueueResponse {
	var versions *taskqueuepb.TaskQueueVersionSelection
	if len(buildIDs) > 0 {
		versions = &taskqueuepb.TaskQueueVersionSelection{BuildIds: buildIDs}
	}
	//nolint:staticcheck // SA1019 deprecated
	resp, err := f.engine.DescribeTaskQueue(context.Background(), &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: enhancedDescribeTestNamespaceID,
		DescRequest: &workflowservice.DescribeTaskQueueRequest{
			Namespace:              enhancedDescribeTestNamespace,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: enhancedDescribeTestTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			TaskQueueType:          enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			Versions:               versions,
			TaskQueueTypes:         taskQueueTypes,
			ReportPollers:          flags.pollers,
			ReportStats:            flags.stats,
			ReportTaskReachability: flags.reachability,
		},
	})
	require.NoError(t, err)
	return resp
}

// expected is the response for data of the current generation.
func (f *enhancedDescribeFixture) expected(
	buildIDs []string,
	taskQueueTypes []enumspb.TaskQueueType,
	flags enhancedDescribeFlags,
) *matchingservice.DescribeTaskQueueResponse {
	if len(buildIDs) == 0 {
		buildIDs = []string{""}
	}
	versionsInfo := make(map[string]*taskqueuepb.TaskQueueVersionInfo)
	for _, buildID := range buildIDs {
		typesInfo := make(map[int32]*taskqueuepb.TaskQueueTypeInfo)
		for _, taskQueueType := range taskQueueTypes {
			typeInfo := &taskqueuepb.TaskQueueTypeInfo{}
			if flags.pollers {
				typeInfo.Pollers = f.pollers(buildID, taskQueueType, 0, 1)
			}
			if flags.stats {
				typeInfo.Stats = &taskqueuepb.TaskQueueStats{
					ApproximateBacklogCount: f.backlog(buildID, taskQueueType, 0) + f.backlog(buildID, taskQueueType, 1),
					ApproximateBacklogAge:   durationpb.New(0),
				}
			}
			typesInfo[int32(taskQueueType)] = typeInfo
		}
		versionInfo := &taskqueuepb.TaskQueueVersionInfo{TypesInfo: typesInfo}
		if flags.reachability {
			// Without versioning rules, the unversioned queue is the reachable default.
			versionInfo.TaskReachability = enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE
		}
		versionsInfo[buildID] = versionInfo
	}
	//nolint:staticcheck // SA1019 deprecated
	return &matchingservice.DescribeTaskQueueResponse{
		DescResponse: &workflowservice.DescribeTaskQueueResponse{VersionsInfo: versionsInfo},
	}
}

func (f *enhancedDescribeFixture) requireCachedValuesUnmodified(t *testing.T) {
	for _, put := range f.cachePuts {
		protorequire.ProtoEqual(t, put[1], put[0])
	}
}

func TestDescribeTaskQueueEnhanced_ReportFlagsWithinCacheTTL(t *testing.T) {
	t.Parallel()

	var allFlags []enhancedDescribeFlags
	for _, pollers := range []bool{false, true} {
		for _, stats := range []bool{false, true} {
			for _, reachability := range []bool{false, true} {
				allFlags = append(allFlags, enhancedDescribeFlags{pollers: pollers, stats: stats, reachability: reachability})
			}
		}
	}
	fanOutCalls := len(enhancedDescribeBothTypes) * enhancedDescribeTestPartitions

	for _, first := range allFlags {
		for _, second := range allFlags {
			t.Run(fmt.Sprintf("%s then %s", first, second), func(t *testing.T) {
				t.Parallel()
				f := newEnhancedDescribeFixture(t)

				firstResp := f.describe(t, nil, enhancedDescribeBothTypes, first)
				protorequire.ProtoEqual(t, f.expected(nil, enhancedDescribeBothTypes, first), firstResp)
				require.Equal(t, fanOutCalls, f.partitionCalls)
				firstRespSnapshot := common.CloneProto(firstResp)

				// Only the poller and stats flags decide what the partitions are asked for;
				// a request that differs in nothing else reuses the cached partition info.
				wantCalls := fanOutCalls
				if first.pollers != second.pollers || first.stats != second.stats {
					wantCalls += fanOutCalls
				}
				secondResp := f.describe(t, nil, enhancedDescribeBothTypes, second)
				protorequire.ProtoEqual(t, f.expected(nil, enhancedDescribeBothTypes, second), secondResp)
				require.Equal(t, wantCalls, f.partitionCalls)

				// Both shapes are served from the cache now.
				protorequire.ProtoEqual(t,
					f.expected(nil, enhancedDescribeBothTypes, first),
					f.describe(t, nil, enhancedDescribeBothTypes, first))
				protorequire.ProtoEqual(t,
					f.expected(nil, enhancedDescribeBothTypes, second),
					f.describe(t, nil, enhancedDescribeBothTypes, second))
				require.Equal(t, wantCalls, f.partitionCalls)

				protorequire.ProtoEqual(t, firstRespSnapshot, firstResp)
				f.requireCachedValuesUnmodified(t)
			})
		}
	}
}

func TestDescribeTaskQueueEnhanced_CacheExpiry(t *testing.T) {
	t.Parallel()
	f := newEnhancedDescribeFixture(t)
	flags := enhancedDescribeFlags{pollers: true, stats: true}
	fanOutCalls := len(enhancedDescribeBothTypes) * enhancedDescribeTestPartitions

	cachedExpectation := f.expected(nil, enhancedDescribeBothTypes, flags)
	protorequire.ProtoEqual(t, cachedExpectation, f.describe(t, nil, enhancedDescribeBothTypes, flags))
	require.Equal(t, fanOutCalls, f.partitionCalls)

	f.generation++
	f.timeSource.Advance(enhancedDescribeTestCacheTTL - time.Nanosecond)
	protorequire.ProtoEqual(t, cachedExpectation, f.describe(t, nil, enhancedDescribeBothTypes, flags))
	require.Equal(t, fanOutCalls, f.partitionCalls)

	f.timeSource.Advance(time.Second)
	protorequire.ProtoEqual(t,
		f.expected(nil, enhancedDescribeBothTypes, flags),
		f.describe(t, nil, enhancedDescribeBothTypes, flags))
	require.Equal(t, 2*fanOutCalls, f.partitionCalls)
	f.requireCachedValuesUnmodified(t)
}

func TestDescribeTaskQueueEnhanced_CacheIsolation(t *testing.T) {
	t.Parallel()
	f := newEnhancedDescribeFixture(t)
	flags := enhancedDescribeFlags{pollers: true, stats: true}
	workflowOnly := []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW}
	activityOnly := []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY}

	steps := []struct {
		name           string
		buildIDs       []string
		taskQueueTypes []enumspb.TaskQueueType
		// wantCalls is the number of partition calls the step adds.
		wantCalls int
	}{
		{name: "first build ID and type", buildIDs: []string{"A"}, taskQueueTypes: workflowOnly, wantCalls: enhancedDescribeTestPartitions},
		// The workflow info is cached but the activity info is not: the fan-out replaces the
		// cached part instead of adding to it.
		{name: "second type", buildIDs: []string{"A"}, taskQueueTypes: enhancedDescribeBothTypes, wantCalls: 2 * enhancedDescribeTestPartitions},
		{name: "second build ID", buildIDs: []string{"A", "BB"}, taskQueueTypes: enhancedDescribeBothTypes, wantCalls: 2 * enhancedDescribeTestPartitions},
		{name: "cached build ID and type", buildIDs: []string{"BB"}, taskQueueTypes: activityOnly},
		{name: "other cached build ID and type", buildIDs: []string{"A"}, taskQueueTypes: workflowOnly},
	}
	var responses, snapshots []*matchingservice.DescribeTaskQueueResponse
	for _, step := range steps {
		wantCalls := f.partitionCalls + step.wantCalls
		resp := f.describe(t, step.buildIDs, step.taskQueueTypes, flags)
		protorequire.ProtoEqual(t, f.expected(step.buildIDs, step.taskQueueTypes, flags), resp)
		require.Equal(t, wantCalls, f.partitionCalls, step.name)
		responses = append(responses, resp)
		snapshots = append(snapshots, common.CloneProto(resp))
	}
	for i, resp := range responses {
		protorequire.ProtoEqual(t, snapshots[i], resp)
	}
	f.requireCachedValuesUnmodified(t)
}
