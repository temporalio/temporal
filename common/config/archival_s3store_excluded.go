//go:build temporal_minimal && !temporal_with_s3store

package config

import (
	"errors"
	"fmt"
)

func (s *S3Archiver) validate(path string) error {
	if err := s.Validate(); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// Validate fails if s is set, since the S3 archiver is excluded from this build.
func (s *S3Archiver) Validate() error {
	if s != nil {
		return errors.New("S3 archival is not compiled into this server; remove this config or rebuild with the temporal_with_s3store build tag")
	}
	return nil
}
