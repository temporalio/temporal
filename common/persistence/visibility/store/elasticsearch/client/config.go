package client

import (
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
)

// The configuration moved to esconfig, which doesn't depend on the Elasticsearch client. These
// aliases keep the old names working for existing callers.
// TODO: remove this file once external callers use esconfig.

const (
	// VisibilityAppName is an alias of esconfig.VisibilityAppName.
	//
	// Deprecated: use esconfig.VisibilityAppName.
	VisibilityAppName = esconfig.VisibilityAppName
	// SecondaryVisibilityAppName is an alias of esconfig.SecondaryVisibilityAppName.
	//
	// Deprecated: use esconfig.SecondaryVisibilityAppName.
	SecondaryVisibilityAppName = esconfig.SecondaryVisibilityAppName
)

type (
	// Config is an alias of esconfig.Config.
	//
	// Deprecated: use esconfig.Config.
	Config = esconfig.Config
	// ESAWSRequestSigningConfig is an alias of esconfig.ESAWSRequestSigningConfig.
	//
	// Deprecated: use esconfig.ESAWSRequestSigningConfig.
	ESAWSRequestSigningConfig = esconfig.ESAWSRequestSigningConfig
	// ESAWSStaticCredentialProvider is an alias of esconfig.ESAWSStaticCredentialProvider.
	//
	// Deprecated: use esconfig.ESAWSStaticCredentialProvider.
	ESAWSStaticCredentialProvider = esconfig.ESAWSStaticCredentialProvider
)
