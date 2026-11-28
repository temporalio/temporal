//go:build !temporal_minimal || temporal_with_s3store

package config

import (
	"cmp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func TestS3ArchiverValidate(t *testing.T) {
	t.Parallel()
	for _, storageClass := range append(types.StorageClass("").Values(), "") {
		t.Run(cmp.Or(string(storageClass), "default"), func(t *testing.T) {
			t.Parallel()
			s3config := &S3Archiver{StorageClass: string(storageClass)}
			require.NoError(t, s3config.Validate())
		})
	}
	for _, storageClass := range []string{"INVALID", "standard_ia", " STANDARD_IA"} {
		t.Run(storageClass, func(t *testing.T) {
			t.Parallel()
			s3config := &S3Archiver{StorageClass: storageClass}
			require.ErrorContains(t, s3config.Validate(), "invalid S3 storageClass")
		})
	}
}

func TestArchivalValidateS3StorageClass(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name            string
		historyClass    string
		visibilityClass string
		errorContains   string
	}{
		{name: "default"},
		{name: "separate classes", historyClass: "STANDARD_IA", visibilityClass: "INTELLIGENT_TIERING"},
		{name: "invalid history", historyClass: "invalid", errorContains: "archival.history.provider.s3store: invalid S3 storageClass"},
		{name: "invalid visibility", visibilityClass: "invalid", errorContains: "archival.visibility.provider.s3store: invalid S3 storageClass"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			archival := &Archival{
				History: HistoryArchival{
					State:    ArchivalEnabled,
					Provider: &HistoryArchiverProvider{S3store: &S3Archiver{StorageClass: testCase.historyClass}},
				},
				Visibility: VisibilityArchival{
					State:    ArchivalEnabled,
					Provider: &VisibilityArchiverProvider{S3store: &S3Archiver{StorageClass: testCase.visibilityClass}},
				},
			}
			defaults := &ArchivalNamespaceDefaults{
				History:    HistoryArchivalNamespaceDefaults{URI: "s3://bucket/history"},
				Visibility: VisibilityArchivalNamespaceDefaults{URI: "s3://bucket/visibility"},
			}
			err := archival.Validate(defaults)
			if testCase.errorContains != "" {
				require.ErrorContains(t, err, testCase.errorContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
