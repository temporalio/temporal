//go:build temporal_minimal && !temporal_with_s3store

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchivalValidate_S3storeExcluded(t *testing.T) {
	for name, tc := range map[string]struct {
		archival *Archival
		path     string
	}{
		"history": {
			archival: &Archival{
				History:    HistoryArchival{State: ArchivalEnabled, Provider: &HistoryArchiverProvider{S3store: &S3Archiver{}}},
				Visibility: VisibilityArchival{State: ArchivalDisabled},
			},
			path: "archival.history.provider.s3store",
		},
		"visibility": {
			archival: &Archival{
				History:    HistoryArchival{State: ArchivalDisabled},
				Visibility: VisibilityArchival{State: ArchivalEnabled, Provider: &VisibilityArchiverProvider{S3store: &S3Archiver{}}},
			},
			path: "archival.visibility.provider.s3store",
		},
	} {
		t.Run(name, func(t *testing.T) {
			archival := tc.archival
			namespaceDefaults := &ArchivalNamespaceDefaults{}
			if archival.History.Provider != nil {
				namespaceDefaults.History.URI = "s3://bucket/history"
			}
			if archival.Visibility.Provider != nil {
				namespaceDefaults.Visibility.URI = "s3://bucket/visibility"
			}
			require.EqualError(t, archival.Validate(namespaceDefaults),
				tc.path+": S3 archival is not compiled into this server; remove this config or rebuild with the temporal_with_s3store build tag")
		})
	}
}
