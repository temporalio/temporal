//go:build temporal_minimal && !temporal_with_s3store

package provider

import (
	"fmt"

	"go.temporal.io/server/common/archiver"
)

// errS3storeExcluded is returned for the s3 scheme, whose archiver is excluded from this build.
// It doesn't mention the build tag because it can reach API clients.
// Operators get the build tag from the config validation error instead.
var errS3storeExcluded = fmt.Errorf("%w %q: S3 archival is not available on this server", ErrUnknownScheme, archiver.URISchemeS3store)

func (p *archiverProvider) newS3storeHistoryArchiver() (archiver.HistoryArchiver, error) {
	return nil, errS3storeExcluded
}

func (p *archiverProvider) newS3storeVisibilityArchiver() (archiver.VisibilityArchiver, error) {
	return nil, errS3storeExcluded
}
