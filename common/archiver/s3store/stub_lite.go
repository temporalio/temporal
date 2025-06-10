//go:build lite

package s3store

import (
	"errors"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

// URIScheme is the scheme for the s3 implementation
const URIScheme = "s3"

// NewHistoryArchiver returns an error because the s3store implementation is disabled in lite builds.
func NewHistoryArchiver(persistence.ExecutionManager, log.Logger, metrics.Handler, *config.S3Archiver) (archiver.HistoryArchiver, error) {
	return nil, errors.New("s3 archiver disabled in lite build")
}

// NewVisibilityArchiver returns an error because the s3store implementation is disabled in lite builds.
func NewVisibilityArchiver(log.Logger, metrics.Handler, *config.S3Archiver) (archiver.VisibilityArchiver, error) {
	return nil, errors.New("s3 archiver disabled in lite build")
}
