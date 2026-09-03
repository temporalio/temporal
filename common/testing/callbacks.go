package testing

import (
	"regexp"

	"go.temporal.io/server/common/callbacks"
)

// NewCallbacksValidatorConfig returns a valid callbacks.ValidatorConfig that can be
// used to create a new callbacks.Validator. Tests can override fields to set arbitrary
// limits for aspects of the Validator to be tested.
func NewCallbacksValidatorConfig() callbacks.ValidatorConfig {
	allowAllAddresses := callbacks.AddressMatchRules{
		Rules: []callbacks.AddressMatchRule{
			{Regexp: regexp.MustCompile(`.*`), AllowInsecure: true},
		},
	}
	return callbacks.ValidatorConfig{
		MaxCallbacksPerExecution:              func(string) int { return 10 },
		MaxIDLengthLimit:                      func() int { return 100 },
		URLMaxLength:                          func(string) int { return 1000 },
		HeaderMaxSize:                         func(string) int { return 4096 },
		EndpointRules:                         func(string) callbacks.AddressMatchRules { return allowAllAddresses },
		MaxServiceNameLength:                  func(string) int { return 100 },
		MaxOperationNameLength:                func(string) int { return 100 },
		NexusHandlerSourceContextMaxSize:      func(string) int { return 1000 },
		TotalNexusHandlerSourceContextMaxSize: func(string) int { return 2000 },
	}
}
