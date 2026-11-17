//go:build !temporal_minimal || temporal_with_gstorage

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchivalValidate_Gstorage(t *testing.T) {
	archival := &Archival{
		History: HistoryArchival{
			State:    ArchivalEnabled,
			Provider: &HistoryArchiverProvider{Gstorage: &GstorageArchiver{}},
		},
		Visibility: VisibilityArchival{
			State:    ArchivalEnabled,
			Provider: &VisibilityArchiverProvider{Gstorage: &GstorageArchiver{}},
		},
	}
	namespaceDefaults := &ArchivalNamespaceDefaults{
		History:    HistoryArchivalNamespaceDefaults{URI: "gs://bucket/history"},
		Visibility: VisibilityArchivalNamespaceDefaults{URI: "gs://bucket/visibility"},
	}
	require.NoError(t, archival.Validate(namespaceDefaults))
}
