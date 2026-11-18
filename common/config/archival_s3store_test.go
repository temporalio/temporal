//go:build !temporal_minimal || temporal_with_s3store

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchivalValidate_S3store(t *testing.T) {
	archival := &Archival{
		History: HistoryArchival{
			State:    ArchivalEnabled,
			Provider: &HistoryArchiverProvider{S3store: &S3Archiver{}},
		},
		Visibility: VisibilityArchival{
			State:    ArchivalEnabled,
			Provider: &VisibilityArchiverProvider{S3store: &S3Archiver{}},
		},
	}
	namespaceDefaults := &ArchivalNamespaceDefaults{
		History:    HistoryArchivalNamespaceDefaults{URI: "s3://bucket/history"},
		Visibility: VisibilityArchivalNamespaceDefaults{URI: "s3://bucket/visibility"},
	}
	require.NoError(t, archival.Validate(namespaceDefaults))
}
