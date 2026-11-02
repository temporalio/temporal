package config

import (
	"errors"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	// ArchivalEnabled is the state for enabling archival
	ArchivalEnabled = "enabled"
	// ArchivalDisabled is the state for disabling archival
	ArchivalDisabled = "disabled"
	// ArchivalPaused is the state for pausing archival
	ArchivalPaused = "paused"
)

// Validate validates the archival config
func (a *Archival) Validate(namespaceDefaults *ArchivalNamespaceDefaults) error {
	if !isArchivalConfigValid(a.History.State, a.History.EnableRead, namespaceDefaults.History.State, namespaceDefaults.History.URI, a.History.Provider != nil) {
		return errors.New("invalid history archival config")
	}

	if !isArchivalConfigValid(a.Visibility.State, a.Visibility.EnableRead, namespaceDefaults.Visibility.State, namespaceDefaults.Visibility.URI, a.Visibility.Provider != nil) {
		return errors.New("invalid visibility archival config")
	}

	if err := a.History.Provider.validate(); err != nil {
		return err
	}
	return a.Visibility.Provider.validate()
}

func (p *HistoryArchiverProvider) validate() error {
	if p == nil {
		return nil
	}
	if err := p.S3store.Validate(); err != nil {
		return fmt.Errorf("archival.history.provider.s3store: %w", err)
	}
	return p.Gstorage.validate("archival.history.provider.gstorage")
}

func (p *VisibilityArchiverProvider) validate() error {
	if p == nil {
		return nil
	}
	if err := p.S3store.Validate(); err != nil {
		return fmt.Errorf("archival.visibility.provider.s3store: %w", err)
	}
	return p.Gstorage.validate("archival.visibility.provider.gstorage")
}

// Validate validates the S3 archiver configuration.
func (s *S3Archiver) Validate() error {
	if s == nil || s.StorageClass == "" {
		return nil
	}
	if !slices.Contains(types.StorageClass("").Values(), types.StorageClass(s.StorageClass)) {
		return fmt.Errorf("invalid S3 storageClass %q", s.StorageClass)
	}
	return nil
}

func isArchivalConfigValid(
	clusterStatus string,
	enableRead bool,
	namespaceDefaultStatus string,
	domianDefaultURI string,
	specifiedProvider bool,
) bool {
	archivalEnabled := clusterStatus == ArchivalEnabled
	URISet := len(domianDefaultURI) != 0

	validEnable := archivalEnabled && URISet && specifiedProvider
	validDisabled := !archivalEnabled && !enableRead && namespaceDefaultStatus != ArchivalEnabled && !URISet && !specifiedProvider
	return validEnable || validDisabled
}
