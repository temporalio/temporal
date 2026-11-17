package testing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/links"
)

// NewLinksValidatorConfig returns a valid links.ValidatorConfig that can be used to create a new
// links.Validator. Tests can override fields to set arbitrary limits for aspects of the Validator
// to be tested.
func NewLinksValidatorConfig() links.ValidatorConfig {
	return links.ValidatorConfig{
		MaxLinksPerRequest:   func(string) int { return 10 },
		LinkMaxSize:          func(string) int { return 4000 },
		MaxLinksPerExecution: func(string) int { return 2000 },
	}
}

// NewLinksValidator returns a new [links.Validator] using cfg. Tests that need custom limits start
// from [NewLinksValidatorConfig] and override individual fields.
func NewLinksValidator(t testing.TB, cfg links.ValidatorConfig) links.Validator {
	t.Helper()

	v, err := links.NewValidator(cfg)
	require.NoError(t, err)
	return v
}
