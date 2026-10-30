package history

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestScavengerPageWaitsForBranchOutcomes(t *testing.T) {
	t.Parallel()
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal=%v", terminal), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s, db, client := newCheckpointScavenger(t)
				s.hbd = ScavengerHeartbeatDetails{CurrentPage: 7, NextPageToken: []byte("current")}
				var next []byte
				if !terminal {
					next = []byte("next")
				}
				db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
					PageSize: pageSize, NextPageToken: []byte("current"),
				}).Return(&persistence.GetAllHistoryTreeBranchesResponse{
					Branches: checkpointBranches(3), NextPageToken: next,
				}, nil)
				if !terminal {
					db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
						PageSize: pageSize, NextPageToken: next,
					}).Return(&persistence.GetAllHistoryTreeBranchesResponse{}, nil)
				}
				entered, release := make(chan struct{}), make(chan struct{})
				releaseWorker := sync.OnceFunc(func() { close(release) })
				defer releaseWorker()
				client.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *historyservice.DescribeMutableStateRequest, _ ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {
						if req.Execution.RunId == "run-0" {
							close(entered)
							await.Rcv(t, release)
						}
						if req.Execution.RunId == "run-1" {
							return nil, serviceerror.NewUnavailable("branch unavailable")
						}
						return nil, nil
					}).Times(3)
				done := make(chan ScavengerHeartbeatDetails, 1)
				go func() {
					details, _ := s.Run(t.Context())
					done <- details
				}()
				await.Rcv(t, entered)
				synctest.Wait()
				s.Lock()
				checkpoint := s.hbd
				s.Unlock()
				require.Equal(t, []byte("current"), checkpoint.NextPageToken)
				require.Equal(t, 8, checkpoint.CurrentPage)
				require.Equal(t, 1, checkpoint.SuccessCount)
				require.Equal(t, 1, checkpoint.ErrorCount)
				releaseWorker()
				result := await.Rcv(t, done)
				require.Empty(t, result.NextPageToken)
				require.Equal(t, 2, result.SuccessCount)
				require.Equal(t, 1, result.ErrorCount)
			})
		})
	}
}

func newCheckpointScavenger(t *testing.T) (*Scavenger, *persistence.MockExecutionManager, *historyservicemock.MockHistoryServiceClient) {
	t.Helper()
	ctrl := gomock.NewController(t)
	db := persistence.NewMockExecutionManager(ctrl)
	client := historyservicemock.NewMockHistoryServiceClient(ctrl)
	s := NewScavenger(512, db, 100, client, nil, nil, ScavengerHeartbeatDetails{},
		dynamicconfig.GetDurationPropertyFn(time.Hour), dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetBoolPropertyFn(false), metrics.NoopMetricsHandler, log.NewNoopLogger(), serialization.NewSerializer())
	s.isInTest = true
	limiter := quotas.NewMockRateLimiter(ctrl)
	limiter.EXPECT().Wait(gomock.Any()).Return(nil).AnyTimes()
	s.rateLimiter = limiter
	return s, db, client
}

func checkpointBranches(count int) []persistence.HistoryBranchDetail {
	branches := make([]persistence.HistoryBranchDetail, count)
	for i := range branches {
		branches[i] = persistence.HistoryBranchDetail{
			BranchInfo: &persistencespb.HistoryBranch{TreeId: "tree", BranchId: fmt.Sprintf("branch-%d", i)},
			ForkTime:   timestamp.TimePtr(time.Now().Add(-2 * time.Hour)),
			Info:       persistence.BuildHistoryGarbageCleanupInfo("namespace", "workflow", fmt.Sprintf("run-%d", i)),
		}
	}
	return branches
}

func TestScavengerPageCancellation(t *testing.T) {
	t.Parallel()
	for _, count := range []int{2 * numWorker, 2*pageSize + numWorker + 1} {
		t.Run(fmt.Sprintf("rows=%d", count), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s, db, client := newCheckpointScavenger(t)
				s.hbd.NextPageToken = []byte("current")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).Return(
					&persistence.GetAllHistoryTreeBranchesResponse{Branches: checkpointBranches(count), NextPageToken: []byte("next")}, nil)
				entered := make(chan struct{})
				enter := sync.OnceFunc(func() { close(entered) })
				client.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, _ *historyservice.DescribeMutableStateRequest, _ ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {
						enter()
						<-ctx.Done()
						return nil, ctx.Err()
					}).AnyTimes()
				done := make(chan ScavengerHeartbeatDetails, 1)
				go func() {
					details, _ := s.Run(ctx)
					done <- details
				}()
				await.Rcv(t, entered)
				synctest.Wait()
				cancel()
				synctest.Wait()
				result := await.Rcv(t, done)
				require.Equal(t, []byte("current"), result.NextPageToken)
				require.Positive(t, result.ErrorCount)
				require.Zero(t, result.SuccessCount)
			})
		})
	}
}

func TestScavengerPagePartialDispatch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, db, client := newCheckpointScavenger(t)
		s.hbd.NextPageToken = []byte("current")
		entered, release, failed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseWorker := sync.OnceFunc(func() { close(release) })
		defer releaseWorker()
		limiter := quotas.NewMockRateLimiter(gomock.NewController(t))
		s.rateLimiter = limiter
		gomock.InOrder(
			limiter.EXPECT().Wait(gomock.Any()).Return(nil),
			limiter.EXPECT().Wait(gomock.Any()).DoAndReturn(func(context.Context) error {
				await.Rcv(t, entered)
				close(failed)
				return serviceerror.NewUnavailable("limiter unavailable")
			}),
		)
		db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).Return(
			&persistence.GetAllHistoryTreeBranchesResponse{Branches: checkpointBranches(2), NextPageToken: []byte("next")}, nil)
		client.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, *historyservice.DescribeMutableStateRequest, ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {
				close(entered)
				await.Rcv(t, release)
				return nil, nil
			})
		done := make(chan ScavengerHeartbeatDetails, 1)
		go func() {
			details, _ := s.Run(t.Context())
			done <- details
		}()
		await.Rcv(t, failed)
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("Run returned before the dispatched branch finished")
		default:
		}
		releaseWorker()
		result := await.Rcv(t, done)
		require.Equal(t, []byte("current"), result.NextPageToken)
		require.Equal(t, 1, result.SuccessCount)
	})
}

func TestScavengerPageOversizedResponse(t *testing.T) {
	t.Parallel()
	s, db, client := newCheckpointScavenger(t)
	count := 2*pageSize + numWorker + 1
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).Return(
		&persistence.GetAllHistoryTreeBranchesResponse{Branches: checkpointBranches(count)}, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).Return(nil, nil).Times(count)
	result, err := s.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, count, result.SuccessCount)
	require.Empty(t, result.NextPageToken)
}

func TestScavengerPageEmptyResponses(t *testing.T) {
	t.Parallel()
	s, db, _ := newCheckpointScavenger(t)
	gomock.InOrder(
		db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{PageSize: pageSize}).Return(
			&persistence.GetAllHistoryTreeBranchesResponse{NextPageToken: []byte("next")}, nil),
		db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{PageSize: pageSize, NextPageToken: []byte("next")}).Return(
			&persistence.GetAllHistoryTreeBranchesResponse{}, nil),
	)
	result, err := s.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, ScavengerHeartbeatDetails{CurrentPage: 2}, result)
}

func TestScavengerPageTokenOwnership(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, db, client := newCheckpointScavenger(t)
		s.hbd.NextPageToken = []byte("current")
		next := []byte("next")
		db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, req *persistence.GetAllHistoryTreeBranchesRequest) (*persistence.GetAllHistoryTreeBranchesResponse, error) {
				req.NextPageToken[0] = 'X'
				return &persistence.GetAllHistoryTreeBranchesResponse{Branches: checkpointBranches(1), NextPageToken: next}, nil
			})
		db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{PageSize: pageSize, NextPageToken: []byte("next")}).DoAndReturn(
			func(_ context.Context, req *persistence.GetAllHistoryTreeBranchesRequest) (*persistence.GetAllHistoryTreeBranchesResponse, error) {
				req.NextPageToken[0] = 'Z'
				return nil, serviceerror.NewUnavailable("page unavailable")
			})
		entered, release := make(chan struct{}), make(chan struct{})
		releaseWorker := sync.OnceFunc(func() { close(release) })
		defer releaseWorker()
		client.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, *historyservice.DescribeMutableStateRequest, ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {
				close(entered)
				await.Rcv(t, release)
				return nil, nil
			})
		done := make(chan ScavengerHeartbeatDetails, 1)
		go func() {
			details, _ := s.Run(t.Context())
			done <- details
		}()
		await.Rcv(t, entered)
		synctest.Wait()
		s.Lock()
		checkpoint := string(s.hbd.NextPageToken)
		s.Unlock()
		require.Equal(t, "current", checkpoint)
		next[0] = 'Y'
		releaseWorker()
		result := await.Rcv(t, done)
		require.Equal(t, []byte("next"), result.NextPageToken)
	})
}
