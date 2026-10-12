//go:build temporal_minimal && !temporal_with_gstorage

package provider

import (
	"fmt"

	"go.temporal.io/server/common/archiver"
)

// The Google Cloud Storage archiver is excluded from this build.
func init() {
	err := fmt.Errorf("%w: %q (Google Cloud Storage archival is excluded from this build, see build tag temporal_with_gstorage)", ErrUnknownScheme, "gs")
	optionalHistoryArchivers["gs"] = func(*archiverProvider) (archiver.HistoryArchiver, error) {
		return nil, err
	}
	optionalVisibilityArchivers["gs"] = func(*archiverProvider) (archiver.VisibilityArchiver, error) {
		return nil, err
	}
}
