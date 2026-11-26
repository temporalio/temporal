package history

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
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
)

func TestScavengerRunEnumerationFailure(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	s, db, _ := newScavengerRunTest(ctrl)
	failure := errors.New("history enumeration failed")
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).Return(nil, failure)

	details, err := s.Run(t.Context())

	require.ErrorIs(t, err, failure)
	require.Equal(t, ScavengerHeartbeatDetails{}, details)
}

func TestScavengerRunEmptyContinuation(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			s, db, _ := newScavengerRunTest(ctrl)
			var failure error
			if fail {
				failure = errors.New("next history page unavailable")
			}
			db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{PageSize: pageSize}).Return(
				&persistence.GetAllHistoryTreeBranchesResponse{NextPageToken: []byte("next")}, nil)
			db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{PageSize: pageSize, NextPageToken: []byte("next")}).Return(
				&persistence.GetAllHistoryTreeBranchesResponse{}, failure)

			details, err := s.Run(t.Context())

			if fail {
				require.ErrorIs(t, err, failure)
				require.Equal(t, 1, details.CurrentPage)
				require.Equal(t, []byte("next"), details.NextPageToken)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, details.CurrentPage)
				require.Empty(t, details.NextPageToken)
			}
		})
	}
}

func TestScavengerRunDrainsAfterLoaderFailure(t *testing.T) {
	t.Parallel()
	for _, cancelWhileDraining := range []bool{false, true} {
		name := "live context"
		if cancelWhileDraining {
			name = "canceled context"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				s, db, client := newScavengerRunTest(ctrl)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				entered, failed, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				releaseWorker := sync.OnceFunc(func() { close(release) })
				defer releaseWorker()
				failure := errors.New("rate limiter failed")
				limiter := quotas.NewMockRateLimiter(ctrl)
				s.rateLimiter = limiter
				gomock.InOrder(
					limiter.EXPECT().Wait(gomock.Any()).Return(nil),
					limiter.EXPECT().Wait(gomock.Any()).DoAndReturn(func(context.Context) error {
						await.Rcv(t, entered)
						close(failed)
						return failure
					}),
				)
				db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).Return(
					&persistence.GetAllHistoryTreeBranchesResponse{Branches: []persistence.HistoryBranchDetail{scavengerRunTestBranch(), scavengerRunTestBranch()}}, nil)
				client.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, _ *historyservice.DescribeMutableStateRequest, _ ...any) (*historyservice.DescribeMutableStateResponse, error) {
						close(entered)
						select {
						case <-release:
							return &historyservice.DescribeMutableStateResponse{}, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					})
				done := make(chan scavengerRunResult, 1)
				go func() {
					details, err := s.Run(ctx)
					done <- scavengerRunResult{details, err}
				}()
				await.Rcv(t, failed)
				synctest.Wait()
				select {
				case got := <-done:
					t.Fatalf("Run returned before the dispatched branch finished: %+v", got)
				default:
				}
				if cancelWhileDraining {
					cancel()
				} else {
					releaseWorker()
				}
				got := await.Rcv(t, done)
				require.ErrorIs(t, got.err, failure)
				if cancelWhileDraining {
					require.Equal(t, 1, got.details.ErrorCount)
					require.Zero(t, got.details.SuccessCount)
				} else {
					require.Equal(t, 1, got.details.SuccessCount)
					require.Zero(t, got.details.ErrorCount)
				}
			})
		})
	}
}

func TestScavengerRunCancellationJoinsLoader(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		s, db, _ := newScavengerRunTest(ctrl)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseLoader := sync.OnceFunc(func() { close(release) })
		defer releaseLoader()
		db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, _ *persistence.GetAllHistoryTreeBranchesRequest) (*persistence.GetAllHistoryTreeBranchesResponse, error) {
				close(entered)
				await.Rcv(t, ctx.Done())
				close(canceled)
				await.Rcv(t, release)
				return nil, ctx.Err()
			})
		done := make(chan error, 1)
		go func() { _, err := s.Run(ctx); done <- err }()
		await.Rcv(t, entered)
		cancel()
		await.Rcv(t, canceled)
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Run returned before its loader exited: %v", err)
		default:
		}
		releaseLoader()
		require.ErrorIs(t, await.Rcv(t, done), context.Canceled)
	})
}

func TestScavengerRunCancellationWhileWorkerDrains(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		s, db, client := newScavengerRunTest(ctrl)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		entered := make(chan struct{})
		db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).Return(
			&persistence.GetAllHistoryTreeBranchesResponse{Branches: []persistence.HistoryBranchDetail{scavengerRunTestBranch()}}, nil)
		client.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, _ *historyservice.DescribeMutableStateRequest, _ ...any) (*historyservice.DescribeMutableStateResponse, error) {
				close(entered)
				await.Rcv(t, ctx.Done())
				return nil, ctx.Err()
			})
		done := make(chan scavengerRunResult, 1)
		go func() {
			details, err := s.Run(ctx)
			done <- scavengerRunResult{details, err}
		}()
		await.Rcv(t, entered)
		synctest.Wait()
		cancel()
		got := await.Rcv(t, done)
		require.ErrorIs(t, got.err, context.Canceled)
		require.Equal(t, 1, got.details.ErrorCount)
	})
}

func TestScavengerRunBranchFailureIsNonfatal(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	s, db, client := newScavengerRunTest(ctrl)
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).Return(
		&persistence.GetAllHistoryTreeBranchesResponse{Branches: []persistence.HistoryBranchDetail{scavengerRunTestBranch()}}, nil)
	client.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).Return(nil, errors.New("branch unavailable"))

	details, err := s.Run(t.Context())

	require.NoError(t, err)
	require.Equal(t, 1, details.ErrorCount)
	require.Zero(t, details.SuccessCount)
}

type scavengerRunResult struct {
	details ScavengerHeartbeatDetails
	err     error
}

func newScavengerRunTest(ctrl *gomock.Controller) (*Scavenger, *persistence.MockExecutionManager, *historyservicemock.MockHistoryServiceClient) {
	db := persistence.NewMockExecutionManager(ctrl)
	client := historyservicemock.NewMockHistoryServiceClient(ctrl)
	s := NewScavenger(512, db, 100, client, nil, nil, ScavengerHeartbeatDetails{},
		dynamicconfig.GetDurationPropertyFn(time.Hour), dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetBoolPropertyFn(false), metrics.NoopMetricsHandler, log.NewNoopLogger(), serialization.NewSerializer())
	s.isInTest = true
	return s, db, client
}

func scavengerRunTestBranch() persistence.HistoryBranchDetail {
	return persistence.HistoryBranchDetail{
		BranchInfo: &persistencespb.HistoryBranch{TreeId: treeID1, BranchId: branchID1},
		ForkTime:   timestamp.TimePtr(time.Now().Add(-2 * time.Hour)),
		Info:       persistence.BuildHistoryGarbageCleanupInfo("namespaceID", "workflowID", "runID"),
	}
}
