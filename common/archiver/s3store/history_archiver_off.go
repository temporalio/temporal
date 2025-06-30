//go:build disable_cloud_archival

package s3store

import "go.temporal.io/server/common/archiver"

// NewHistoryArchiver creates a new gcloud storage HistoryArchiver
func NewHistoryArchiver(...any) (archiver.HistoryArchiver, error) {
	panic("cloud archival is disabled via build tag `disable_cloud_archival`")
}
