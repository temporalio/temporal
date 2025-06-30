//go:build disable_cloud_archival

package s3store

import "go.temporal.io/server/common/archiver"

// NewVisibilityArchiver creates a new archiver.VisibilityArchiver based on filestore
func NewVisibilityArchiver(...any) (archiver.VisibilityArchiver, error) {
	panic("cloud archival is disabled via build tag `disable_cloud_archival`")
}
