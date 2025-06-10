//go:build lite

package aws

import (
	"fmt"
	"net/http"

	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
)

func NewAwsHttpClient(config client.ESAWSRequestSigningConfig) (*http.Client, error) {
	return nil, fmt.Errorf("AWS request signing is not supported in lite mode")
}
