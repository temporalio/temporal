package provider

import (
	"go.temporal.io/server/common/archiver"
)

// Optional archivers register themselves by URI scheme from build-tagged files, so that an archiver
// excluded from the build doesn't link its cloud SDK.
type (
	historyArchiverFactory    func(p *archiverProvider) (archiver.HistoryArchiver, error)
	visibilityArchiverFactory func(p *archiverProvider) (archiver.VisibilityArchiver, error)
)

var (
	optionalHistoryArchivers    = map[string]historyArchiverFactory{}
	optionalVisibilityArchivers = map[string]visibilityArchiverFactory{}
)

func (p *archiverProvider) newOptionalHistoryArchiver(scheme string) (archiver.HistoryArchiver, error) {
	if newArchiver, ok := optionalHistoryArchivers[scheme]; ok {
		return newArchiver(p)
	}
	return nil, ErrUnknownScheme
}

func (p *archiverProvider) newOptionalVisibilityArchiver(scheme string) (archiver.VisibilityArchiver, error) {
	if newArchiver, ok := optionalVisibilityArchivers[scheme]; ok {
		return newArchiver(p)
	}
	return nil, ErrUnknownScheme
}
