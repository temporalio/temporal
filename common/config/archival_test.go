package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchivalValidate_Disabled(t *testing.T) {
	archival := &Archival{
		History:    HistoryArchival{State: ArchivalDisabled},
		Visibility: VisibilityArchival{State: ArchivalDisabled},
	}
	require.NoError(t, archival.Validate(&ArchivalNamespaceDefaults{}))
}

func TestArchivalValidate_Filestore(t *testing.T) {
	archival := &Archival{
		History: HistoryArchival{
			State:    ArchivalEnabled,
			Provider: &HistoryArchiverProvider{Filestore: &FilestoreArchiver{}},
		},
		Visibility: VisibilityArchival{
			State:    ArchivalEnabled,
			Provider: &VisibilityArchiverProvider{Filestore: &FilestoreArchiver{}},
		},
	}
	namespaceDefaults := &ArchivalNamespaceDefaults{
		History:    HistoryArchivalNamespaceDefaults{URI: "file:///tmp/history"},
		Visibility: VisibilityArchivalNamespaceDefaults{URI: "file:///tmp/visibility"},
	}
	require.NoError(t, archival.Validate(namespaceDefaults))
}
