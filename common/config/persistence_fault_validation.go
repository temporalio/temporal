package config

import (
	"fmt"

	"go.temporal.io/server/common/probability"
)

func (c FaultInjectionMethodConfig) Validate() error {
	return probability.ValidateProbabilities(c.Errors)
}

func (fi *FaultInjection) Validate() error {
	if fi == nil {
		return nil
	}
	for store, cfg := range fi.Targets.DataStores {
		for method, cfg := range cfg.Methods {
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("targets.dataStores.%s.methods.%s: %w", store, method, err)
			}
		}
	}
	return nil
}
