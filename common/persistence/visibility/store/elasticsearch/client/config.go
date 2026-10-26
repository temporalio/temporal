package client

import (
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
)

// The configuration moved to esconfig, which doesn't depend on the Elasticsearch client. These
// aliases keep the old names working for existing callers.
// TODO: remove this file once external callers use esconfig.

const (
	// Deprecated: use esconfig.VisibilityAppName.
	VisibilityAppName = esconfig.VisibilityAppName
	// Deprecated: use esconfig.SecondaryVisibilityAppName.
	SecondaryVisibilityAppName = esconfig.SecondaryVisibilityAppName
)

type (
	// Deprecated: use esconfig.Config.
	Config = esconfig.Config
	// Deprecated: use esconfig.ESAWSRequestSigningConfig.
	ESAWSRequestSigningConfig = esconfig.ESAWSRequestSigningConfig
	// Deprecated: use esconfig.ESAWSStaticCredentialProvider.
	ESAWSStaticCredentialProvider = esconfig.ESAWSStaticCredentialProvider
)
