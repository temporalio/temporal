//go:build temporal_minimal && !temporal_with_gstorage

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchivalValidate_GstorageExcluded(t *testing.T) {
	for name, tc := range map[string]struct {
		archival *Archival
		path     string
	}{
		"history": {
			archival: &Archival{
				History:    HistoryArchival{State: ArchivalEnabled, Provider: &HistoryArchiverProvider{Gstorage: &GstorageArchiver{}}},
				Visibility: VisibilityArchival{State: ArchivalDisabled},
			},
			path: "archival.history.provider.gstorage",
		},
		"visibility": {
			archival: &Archival{
				History:    HistoryArchival{State: ArchivalDisabled},
				Visibility: VisibilityArchival{State: ArchivalEnabled, Provider: &VisibilityArchiverProvider{Gstorage: &GstorageArchiver{}}},
			},
			path: "archival.visibility.provider.gstorage",
		},
	} {
		t.Run(name, func(t *testing.T) {
			archival := tc.archival
			namespaceDefaults := &ArchivalNamespaceDefaults{}
			if archival.History.Provider != nil {
				namespaceDefaults.History.URI = "gs://bucket/history"
			}
			if archival.Visibility.Provider != nil {
				namespaceDefaults.Visibility.URI = "gs://bucket/visibility"
			}
			require.EqualError(t, archival.Validate(namespaceDefaults),
				tc.path+": Google Cloud Storage archival is not compiled into this server; remove this config or rebuild with the temporal_with_gstorage build tag")
		})
	}
}
