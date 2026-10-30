package matching

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

func TestFormatNexusRequestTimeout(t *testing.T) {
	tests := []struct {
		name      string
		remaining time.Duration
		want      string
	}{
		{
			name:      "positive milliseconds",
			remaining: 1500 * time.Millisecond,
			want:      "1500ms",
		},
		{
			name:      "hours uses millisecond unit",
			remaining: 2 * time.Hour,
			want:      "7200000ms",
		},
		{
			name:      "multi-unit duration",
			remaining: time.Minute + 30*time.Second,
			want:      "90000ms",
		},
		{
			name:      "sub-millisecond truncates to zero",
			remaining: 88 * time.Microsecond,
			want:      "0ms",
		},
		{
			name:      "zero",
			remaining: 0,
			want:      "0ms",
		},
		{
			name:      "negative microseconds clamped",
			remaining: -88 * time.Microsecond,
			want:      "0ms",
		},
		{
			name:      "negative milliseconds clamped",
			remaining: -1500 * time.Millisecond,
			want:      "0ms",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatNexusRequestTimeout(tt.remaining)
			require.Equal(t, tt.want, got)
			_, err := nexusrpc.ParseDuration(got)
			require.NoError(t, err)
		})
	}
}

func (s *matchingEngineSuite) TestPollNexusTaskQueue_RequestTimeoutHeader() {
	testCases := []struct {
		name            string
		deadline        time.Time
		wantExact       string
		wantApprox      time.Duration
		wantApproxDelta time.Duration
	}{
		{
			name:      "expired deadline is clamped to zero",
			deadline:  time.Now().Add(-2 * time.Millisecond),
			wantExact: "0ms",
		},
		{
			name:            "hours formatted as milliseconds",
			deadline:        time.Now().Add(2 * time.Hour),
			wantApprox:      2 * time.Hour,
			wantApproxDelta: 5 * time.Second,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			namespaceID := s.ns.ID().String()
			taskQueueName := "test-nexus-timeout-tq-" + tc.name

			dispatchReq := &matchingservice.DispatchNexusTaskRequest{
				NamespaceId: namespaceID,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Request: &nexuspb.Request{
					Header: map[string]string{},
				},
			}

			nexusTask := newInternalNexusTask(
				"test-timeout-task-id",
				tc.deadline,
				time.Time{},
				dispatchReq,
			)

			partition, err := tqid.PartitionFromProto(
				&taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				namespaceID,
				enumspb.TASK_QUEUE_TYPE_NEXUS,
			)
			s.Require().NoError(err)

			mockPM := NewMocktaskQueuePartitionManager(s.controller)
			mockPM.EXPECT().WaitUntilInitialized(gomock.Any()).Return(nil)
			mockPM.EXPECT().LongPollExpirationInterval().Return(time.Minute)
			mockPM.EXPECT().Stop(gomock.Any()).AnyTimes()
			mockPM.EXPECT().PollTask(gomock.Any(), gomock.Any()).Return(nexusTask, false, nil)

			s.matchingEngine.partitionsLock.Lock()
			s.matchingEngine.partitions[partition.Key()] = mockPM
			s.matchingEngine.partitionsLock.Unlock()
			s.matchingEngine.nexusResults = collection.NewSyncMap[string, chan *nexusResult]()
			s.matchingEngine.outstandingPollers = collection.NewSyncMap[string, context.CancelFunc]()
			s.matchingEngine.shutdownWorkers = cache.New(100, &cache.Options{TTL: 30 * time.Second})

			resp, err := s.matchingEngine.PollNexusTaskQueue(
				context.Background(),
				&matchingservice.PollNexusTaskQueueRequest{
					NamespaceId: namespaceID,
					PollerId:    uuid.NewString(),
					Request: &workflowservice.PollNexusTaskQueueRequest{
						Namespace: string(s.ns.Name()),
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: taskQueueName,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
					},
				},
				metrics.NoopMetricsHandler,
			)
			s.Require().NoError(err)

			header := resp.GetResponse().GetRequest().GetHeader()
			got := header[nexus.HeaderRequestTimeout]
			s.Require().Equal(got, header["Request-Timeout"])

			parsed, err := nexusrpc.ParseDuration(got)
			s.Require().NoError(err)

			if tc.wantExact != "" {
				s.Require().Equal(tc.wantExact, got)
				return
			}
			s.Require().InDelta(float64(tc.wantApprox), float64(parsed), float64(tc.wantApproxDelta))
		})
	}
}
