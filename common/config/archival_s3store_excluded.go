//go:build temporal_minimal && !temporal_with_s3store

package config

import (
	"fmt"
)

// validate fails if s is set, since the S3 archiver is excluded from this build.
func (s *S3Archiver) validate(path string) error {
	if s != nil {
		return fmt.Errorf("%s: S3 archival is not compiled into this server; remove this config or rebuild with the temporal_with_s3store build tag", path)
	}
	return nil
}
