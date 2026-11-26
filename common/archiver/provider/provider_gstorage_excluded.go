//go:build temporal_minimal && !temporal_with_gstorage

package provider

import (
	"fmt"

	"go.temporal.io/server/common/archiver"
)

// errGstorageExcluded is returned for the gs scheme, whose archiver is excluded from this build.
// It doesn't mention the build tag because it can reach API clients.
// Operators get the build tag from the config validation error instead.
var errGstorageExcluded = fmt.Errorf("%w %q: Google Cloud Storage archival is not available on this server", ErrUnknownScheme, archiver.URISchemeGstorage)

func (p *archiverProvider) newGstorageHistoryArchiver() (archiver.HistoryArchiver, error) {
	return nil, errGstorageExcluded
}

func (p *archiverProvider) newGstorageVisibilityArchiver() (archiver.VisibilityArchiver, error) {
	return nil, errGstorageExcluded
}
