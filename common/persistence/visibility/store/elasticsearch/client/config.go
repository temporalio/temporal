package client

import (
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
)

// The configuration moved to esconfig, which doesn't depend on the Elasticsearch client. These
// aliases keep the old names working for existing callers, including saas-temporal.
// TODO: migrate callers to esconfig and remove this file.

const (
	VisibilityAppName          = esconfig.VisibilityAppName
	SecondaryVisibilityAppName = esconfig.SecondaryVisibilityAppName
)

type (
	Config                        = esconfig.Config
	ESAWSRequestSigningConfig     = esconfig.ESAWSRequestSigningConfig
	ESAWSStaticCredentialProvider = esconfig.ESAWSStaticCredentialProvider
)
