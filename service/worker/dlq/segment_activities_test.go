package dlq

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/headers"
	"go.uber.org/mock/gomock"
)

type testSegmentMetadataClient struct {
	segmentClient
	list func(context.Context, historydlq.ListEntriesRequest) (historydlq.EntryPage, error)
}

func (c testSegmentMetadataClient) ListEntries(ctx context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
	return c.list(ctx, req)
}

func segmentJobFixture(t *testing.T) (SegmentJobParams, historydlq.SegmentRef) {
	t.Helper()
	params := SegmentJobParams{
		Key:          Key{TaskCategoryID: 3, SourceCluster: "source", TargetCluster: "target"},
		MaxMessageID: 10,
	}
	queue := historydlq.QueueKey{CategoryID: 3, SourceCluster: "source", TargetCluster: "target"}
	businessID, err := historydlq.SegmentBusinessID(queue, historydlq.TaskIdentity{SourceShardID: 7, TaskID: 1})
	require.NoError(t, err)
	return params, historydlq.SegmentRef{Queue: queue, BusinessID: businessID, RunID: "segment-run"}
}

func TestDiscoverSegmentsQueueScope(t *testing.T) {
	t.Parallel()
	params, segment := segmentJobFixture(t)
	visibilityManager := chasm.NewMockVisibilityManager(gomock.NewController(t))
	visibilityManager.EXPECT().ListExecutions(gomock.Any(), reflect.TypeFor[*historydlq.Segment](), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ reflect.Type, req *chasm.ListExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
			require.Equal(t, headers.CallerTypePreemptable, headers.GetCallerInfo(ctx).CallerType)
			require.Contains(t, req.Query, "HistoryDLQCategory = 3")
			require.Contains(t, req.Query, "HistoryDLQSourceCluster = 'source'")
			require.NotContains(t, req.Query, "HistoryDLQTargetCluster")
			require.Contains(t, req.Query, "HistoryDLQMinMessageID <= 10")
			return &visibilityservice.ListChasmExecutionsResponse{Executions: []*chasmspb.VisibilityExecutionInfo{
				{BusinessId: segment.BusinessID, RunId: segment.RunID},
				{BusinessId: segment.BusinessID, RunId: segment.RunID},
			}}, nil
		})
	component := &workerComponent{currentClusterName: "target", visibilityManager: visibilityManager}
	result, err := component.discoverSegments(headers.SetCallerType(t.Context(), headers.CallerTypePreemptable), params)
	require.NoError(t, err)
	require.Equal(t, []historydlq.SegmentRef{segment}, result)
}

func TestListSegmentEntriesNumericTies(t *testing.T) {
	t.Parallel()
	_, segment := segmentJobFixture(t)
	first := historydlq.MessageRef{
		Queue: segment.Queue, BusinessID: segment.BusinessID, RunID: segment.RunID,
		MessageID: 10, EntryKey: "a",
		EnqueuedAt: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC),
	}
	second := first
	second.EntryKey = "b"
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testSegmentMetadataClient{list: func(ctx context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
			require.Equal(t, headers.CallerTypePreemptable, headers.GetCallerInfo(ctx).CallerType)
			require.Equal(t, segment, req.Segment)
			require.Equal(t, int64(10), req.InclusiveMaxMessageID)
			if len(req.NextPageToken) == 0 {
				return historydlq.EntryPage{Refs: []historydlq.MessageRef{first}, NextPageToken: []byte("next")}, nil
			}
			require.Equal(t, []byte("next"), req.NextPageToken)
			return historydlq.EntryPage{Refs: []historydlq.MessageRef{second}}, nil
		}},
	}
	req := historydlq.ListEntriesRequest{Segment: segment, InclusiveMaxMessageID: 10, PageSize: 1}
	ctx := headers.SetCallerType(t.Context(), headers.CallerTypePreemptable)
	page, err := component.listSegmentEntries(ctx, req)
	require.NoError(t, err)
	require.Equal(t, []historydlq.MessageRef{first}, page.Refs)
	req.NextPageToken = page.NextPageToken
	page, err = component.listSegmentEntries(ctx, req)
	require.NoError(t, err)
	require.Equal(t, historydlq.EntryPage{Refs: []historydlq.MessageRef{second}}, page)
}

func TestListSegmentEntriesLostResponse(t *testing.T) {
	t.Parallel()
	_, segment := segmentJobFixture(t)
	request := historydlq.ListEntriesRequest{Segment: segment, InclusiveMaxMessageID: 10, PageSize: 256}
	expected := historydlq.EntryPage{NextPageToken: []byte("next")}
	calls := 0
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testSegmentMetadataClient{list: func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
			require.Equal(t, request, req)
			calls++
			if calls == 1 {
				return historydlq.EntryPage{}, serviceerror.NewUnavailable("response lost")
			}
			return expected, nil
		}},
	}
	_, err := component.listSegmentEntries(t.Context(), request)
	require.Error(t, err)
	result, err := component.listSegmentEntries(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, expected, result)
	require.Equal(t, 2, calls)
}

func TestListSegmentEntriesDefaults(t *testing.T) {
	t.Parallel()
	_, segment := segmentJobFixture(t)
	request := historydlq.ListEntriesRequest{
		Segment: segment, InclusiveMaxMessageID: 0, NextPageToken: []byte("cursor"),
	}
	expectedRequest := request
	expectedRequest.PageSize = segmentPageSize
	expectedPage := historydlq.EntryPage{NextPageToken: []byte("next")}
	calls := 0
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testSegmentMetadataClient{list: func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
			calls++
			require.Equal(t, expectedRequest, req)
			return expectedPage, nil
		}},
	}
	page, err := component.listSegmentEntries(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, expectedPage, page)
	require.Equal(t, 1, calls)
}

func TestSegmentJobQueueValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*SegmentJobParams)
	}{
		{name: "remote target", change: func(p *SegmentJobParams) { p.Key.TargetCluster = "remote" }},
		{name: "missing source", change: func(p *SegmentJobParams) { p.Key.SourceCluster = "" }},
		{name: "invalid category", change: func(p *SegmentJobParams) { p.Key.TaskCategoryID = 0 }},
		{name: "negative bound", change: func(p *SegmentJobParams) { p.MaxMessageID = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, segment := segmentJobFixture(t)
			tc.change(&params)
			component := &workerComponent{currentClusterName: "target"}
			_, err := component.discoverSegments(t.Context(), params)
			var applicationError *temporal.ApplicationError
			require.ErrorAs(t, err, &applicationError)
			require.True(t, applicationError.NonRetryable())
			segment.Queue = historydlq.QueueKey{
				CategoryID:    int32(params.Key.TaskCategoryID),
				SourceCluster: params.Key.SourceCluster,
				TargetCluster: params.Key.TargetCluster,
			}
			_, err = component.listSegmentEntries(t.Context(), historydlq.ListEntriesRequest{
				Segment: segment, InclusiveMaxMessageID: params.MaxMessageID,
			})
			require.ErrorAs(t, err, &applicationError)
			require.True(t, applicationError.NonRetryable())
		})
	}
}

func TestDiscoverSegmentsFailure(t *testing.T) {
	t.Parallel()
	params, _ := segmentJobFixture(t)
	expected := errors.New("visibility unavailable")
	visibilityManager := chasm.NewMockVisibilityManager(gomock.NewController(t))
	visibilityManager.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, expected)
	component := &workerComponent{currentClusterName: "target", visibilityManager: visibilityManager}
	_, err := component.discoverSegments(t.Context(), params)
	require.ErrorIs(t, err, expected)
}

func TestListSegmentEntriesMissingRun(t *testing.T) {
	t.Parallel()
	_, segment := segmentJobFixture(t)
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testSegmentMetadataClient{list: func(context.Context, historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
			return historydlq.EntryPage{}, serviceerror.NewNotFound("segment run not found")
		}},
	}
	_, err := component.listSegmentEntries(t.Context(), historydlq.ListEntriesRequest{
		Segment: segment, InclusiveMaxMessageID: 10,
	})
	var applicationError *temporal.ApplicationError
	require.ErrorAs(t, err, &applicationError)
	require.True(t, applicationError.NonRetryable())
}
