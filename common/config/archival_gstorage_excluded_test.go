//go:build temporal_minimal && !temporal_with_gstorage

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchivalValidate_GstorageExcluded(t *testing.T) {
	for name, archival := range map[string]*Archival{
		"history": {
			History:    HistoryArchival{State: ArchivalEnabled, Provider: &HistoryArchiverProvider{Gstorage: &GstorageArchiver{}}},
			Visibility: VisibilityArchival{State: ArchivalDisabled},
		},
		"visibility": {
			History:    HistoryArchival{State: ArchivalDisabled},
			Visibility: VisibilityArchival{State: ArchivalEnabled, Provider: &VisibilityArchiverProvider{Gstorage: &GstorageArchiver{}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			namespaceDefaults := &ArchivalNamespaceDefaults{}
			if archival.History.Provider != nil {
				namespaceDefaults.History.URI = "gs://bucket/history"
			}
			if archival.Visibility.Provider != nil {
				namespaceDefaults.Visibility.URI = "gs://bucket/visibility"
			}
			require.ErrorContains(t, archival.Validate(namespaceDefaults), "temporal_with_gstorage")
		})
	}
}
