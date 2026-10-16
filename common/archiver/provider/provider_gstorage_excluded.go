//go:build temporal_minimal && !temporal_with_gstorage

package provider

import (
	"fmt"

	"go.temporal.io/server/common/archiver"
)

// The Google Cloud Storage archiver is excluded from this build.
var errGstorageExcluded = fmt.Errorf("%w: %q (Google Cloud Storage archival is excluded from this build, see build tag temporal_with_gstorage)", ErrUnknownScheme, archiver.URISchemeGstorage)

func (p *archiverProvider) newGstorageHistoryArchiver() (archiver.HistoryArchiver, error) {
	return nil, errGstorageExcluded
}

func (p *archiverProvider) newGstorageVisibilityArchiver() (archiver.VisibilityArchiver, error) {
	return nil, errGstorageExcluded
}
