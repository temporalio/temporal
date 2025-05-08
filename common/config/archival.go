package config

import (
	"errors"
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
