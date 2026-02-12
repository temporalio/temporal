//go:build temporal_minimal && !temporal_with_azblob

package provider

import (
	"fmt"

	"go.temporal.io/server/common/archiver"
)

// errAzblobExcluded is returned for the azblob scheme, whose archiver is excluded from this build.
// It doesn't mention the build tag because it can reach API clients.
// Operators get the build tag from the config validation error instead.
var errAzblobExcluded = fmt.Errorf("%w %q: Azure Blob Storage archival is not available on this server", ErrUnknownScheme, archiver.URISchemeAzblob)

func (p *archiverProvider) newAzblobHistoryArchiver() (archiver.HistoryArchiver, error) {
	return nil, errAzblobExcluded
}

func (p *archiverProvider) newAzblobVisibilityArchiver() (archiver.VisibilityArchiver, error) {
	return nil, errAzblobExcluded
}
