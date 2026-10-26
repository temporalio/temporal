//go:build gomad || gomad_wasm_slim

package provider

import (
	"errors"

	"go.temporal.io/server/common/archiver"
)

// The cloud archivers drag the Google and AWS credential chains, and with
// them os/exec and os/user, into the build. The native gomad build and the
// gomad_wasm_slim WASM guest build keep the schemes so configuration
// validation is unchanged but never construct them; selecting one fails.
const (
	gcloudURIScheme = "gs"
	s3URIScheme     = "s3"
)

var errCloudArchiverUnavailable = errors.New("cloud archivers are not built under the gomad or gomad_wasm_slim build tags")

func newGcloudHistoryArchiver(p *archiverProvider) (archiver.HistoryArchiver, error) {
	if p.historyArchiverConfigs.Gstorage == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return nil, errCloudArchiverUnavailable
}

func newGcloudVisibilityArchiver(p *archiverProvider) (archiver.VisibilityArchiver, error) {
	if p.visibilityArchiverConfigs.Gstorage == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return nil, errCloudArchiverUnavailable
}

func newS3HistoryArchiver(p *archiverProvider) (archiver.HistoryArchiver, error) {
	if p.historyArchiverConfigs.S3store == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return nil, errCloudArchiverUnavailable
}

func newS3VisibilityArchiver(p *archiverProvider) (archiver.VisibilityArchiver, error) {
	if p.visibilityArchiverConfigs.S3store == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return nil, errCloudArchiverUnavailable
}
