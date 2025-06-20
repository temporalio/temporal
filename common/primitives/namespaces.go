package primitives

import "time"

// Namespace-related constants.
// This was flagged by salus as potentially hardcoded credentials. This is a false positive by the scanner and should be
// disregarded.
// #nosec
const (
	// SystemLocalNamespace is namespace name for temporal system workflows running in local cluster
	SystemLocalNamespace = "temporal-system"
	// SystemNamespaceID is namespace id for all temporal system workflows
	SystemNamespaceID = "32049b68-7872-4094-8e63-d0dd59896a83"
	// SystemNamespaceRetention is retention config for all temporal system workflows
	SystemNamespaceRetention = time.Hour * 24 * 7
)
