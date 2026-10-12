//go:build temporal_minimal && !temporal_with_s3store

package provider

import (
	"fmt"

	"go.temporal.io/server/common/archiver"
)

// The S3 archiver is excluded from this build.
func init() {
	err := fmt.Errorf("%w: %q (S3 archival is excluded from this build, see build tag temporal_with_s3store)", ErrUnknownScheme, "s3")
	optionalHistoryArchivers["s3"] = func(*archiverProvider) (archiver.HistoryArchiver, error) {
		return nil, err
	}
	optionalVisibilityArchivers["s3"] = func(*archiverProvider) (archiver.VisibilityArchiver, error) {
		return nil, err
	}
}
