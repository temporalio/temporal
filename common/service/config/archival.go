package config

import (
	"errors"

	"github.com/temporalio/temporal/common"
)

// Validate validates the archival config
func (a *Archival) Validate(namespaceDefaults *ArchivalNamespaceDefaults) error {
	if !isArchivalConfigValid(a.History.Status, a.History.EnableRead, namespaceDefaults.History.Status, namespaceDefaults.History.URI, a.History.Provider != nil) {
		return errors.New("Invalid history archival config")
	}

	if !isArchivalConfigValid(a.Visibility.Status, a.Visibility.EnableRead, namespaceDefaults.Visibility.Status, namespaceDefaults.Visibility.URI, a.Visibility.Provider != nil) {
		return errors.New("Invalid visibility archival config")
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
	archivalEnabled := clusterStatus == common.ArchivalEnabled
	URISet := len(domianDefaultURI) != 0

	validEnable := archivalEnabled && URISet && specifiedProvider
	validDisabled := !archivalEnabled && !enableRead && namespaceDefaultStatus != common.ArchivalEnabled && !URISet && !specifiedProvider
	return validEnable || validDisabled
}
