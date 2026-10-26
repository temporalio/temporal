//go:build gomad

package client

import (
	"errors"
	"net/http"
)

// The AWS SDK credential chain reaches os/exec and os/user; the gomad build
// keeps the configuration surface but cannot sign requests.
func NewAwsHttpClient(config ESAWSRequestSigningConfig) (*http.Client, error) {
	if !config.Enabled {
		return nil, nil
	}
	return nil, errors.New("aws request signing is not built under the gomad build tag")
}
