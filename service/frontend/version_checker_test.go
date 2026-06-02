package frontend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	versionpb "go.temporal.io/api/version/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/persistence"
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
