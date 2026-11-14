package frontend

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	versionpb "go.temporal.io/api/version/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIsUpdateNeeded(t *testing.T) {
	tests := []struct {
		name     string
		metadata *persistence.GetClusterMetadataResponse
		want     bool
	}{
		{
			name: "nil VersionInfo returns true",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: nil,
				},
			},
			want: true,
		},
		{
			name: "nil Current returns true",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current:        nil,
						LastUpdateTime: timestamppb.New(time.Now()),
					},
				},
			},
			want: true,
		},
		{
			name: "different server version returns true",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current: &versionpb.ReleaseInfo{
							Version: "0.0.0-old-version",
						},
						LastUpdateTime: timestamppb.New(time.Now()),
					},
				},
			},
			want: true,
		},
		{
			name: "same version with recent LastUpdateTime returns false",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current: &versionpb.ReleaseInfo{
							Version: headers.ServerVersion,
						},
						LastUpdateTime: timestamppb.New(time.Now()),
					},
				},
			},
			want: false,
		},
		{
			name: "same version with old LastUpdateTime returns true",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current: &versionpb.ReleaseInfo{
							Version: headers.ServerVersion,
						},
						LastUpdateTime: timestamppb.New(time.Now().Add(-2 * time.Hour)),
					},
				},
			},
			want: true,
		},
		{
			name: "same version with nil LastUpdateTime returns false",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current: &versionpb.ReleaseInfo{
							Version: headers.ServerVersion,
						},
						LastUpdateTime: nil,
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isUpdateNeeded(tt.metadata)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestVersionCheckerStopCancelsRequest(t *testing.T) {
	t.Parallel()
	for _, disableBeforeStop := range []bool{false, true} {
		name := "enabled"
		if disableBeforeStop {
			name = "disabled after start"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			enabled := true
			manager := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
			started := make(chan context.Context, 1)
			finished := make(chan error, 1)
			manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).DoAndReturn(
				func(ctx context.Context) (*persistence.GetClusterMetadataResponse, error) {
					started <- ctx
					<-ctx.Done()
					finished <- ctx.Err()
					return nil, ctx.Err()
				},
			)
			checker := NewVersionChecker(&Config{EnableServerVersionCheck: func() bool { return enabled }},
				metrics.NoopMetricsHandler, manager, nil)
			t.Cleanup(func() { enabled = true; checker.Stop() })
			checker.Start()
			requestCtx := await.Rcv(t, started)
			require.Equal(t, headers.SystemBackgroundHighCallerInfo, headers.GetCallerInfo(requestCtx))
			if disableBeforeStop {
				enabled = false
			}
			checker.Stop()
			require.ErrorIs(t, requestCtx.Err(), context.Canceled)
			require.ErrorIs(t, await.Rcv(t, finished), context.Canceled)
		})
	}
}

func TestVersionCheckerConcurrentStartStop(t *testing.T) {
	t.Parallel()
	manager := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
	manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(nil, serviceerror.NewUnavailable("stopped")).AnyTimes()
	for range 100 {
		checker := NewVersionChecker(&Config{EnableServerVersionCheck: func() bool { return true }},
			metrics.NoopMetricsHandler, manager, nil)
		start := make(chan struct{})
		var operations sync.WaitGroup
		operations.Go(func() { <-start; checker.Start() })
		operations.Go(func() { <-start; checker.Stop() })
		close(start)
		operations.Wait()
		checker.cancelFunc()
	}
}
