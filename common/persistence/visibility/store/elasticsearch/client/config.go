package client

import (
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
)

const (
	VisibilityAppName          = esconfig.VisibilityAppName
	SecondaryVisibilityAppName = esconfig.SecondaryVisibilityAppName
)

type (
	Config                        = esconfig.Config
	ESAWSRequestSigningConfig     = esconfig.ESAWSRequestSigningConfig
	ESAWSStaticCredentialProvider = esconfig.ESAWSStaticCredentialProvider
)
