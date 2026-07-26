package dynamicconfig

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"go.temporal.io/server/common/dynamicconfig/configurator"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"gopkg.in/yaml.v3"
)

// ConfiguratorClient is a dynamicconfig.Client backed by the configurator expression
// library. It is the alternative to ConfiguratorEvaluator: instead of hooking into
// Collection's lookup, it plugs in at the existing Client seam and needs no core changes at
// all.
//
// It works because every precedence order ends in the empty Constraints (see
// cmd/tools/gendynamicconfig/main.go), so a single unconstrained ConstrainedValue is matched
// by every setting at every precedence. Expressions are evaluated once per key at load time
// and the resulting slice is cached, so reads cost exactly what they cost today.
//
// The limitation is structural rather than incidental: Client.GetValue receives only a Key.
// It is not told the namespace, task queue, or shard of the call, so an expression that
// references those dimensions has nothing to evaluate against. See TestConfiguratorClient
// for exactly where this bites.
type ConfiguratorClient struct {
	logger  log.Logger
	ambient map[string]any
	values  atomic.Pointer[ConfigValueMap]

	NotifyingClientImpl
}

var _ Client = (*ConfiguratorClient)(nil)
var _ NotifyingClient = (*ConfiguratorClient)(nil)

// NewConfiguratorClient returns a client with no configuration loaded; every key reports no
// values until LoadFile succeeds.
func NewConfiguratorClient(ambient AmbientConstraints, logger log.Logger) *ConfiguratorClient {
	c := &ConfiguratorClient{
		logger:  logger,
		ambient: ambient.asMap(),
	}
	c.values.Store(&ConfigValueMap{})
	return c
}

// GetValue implements Client. It is a single atomic load and a map lookup, and returns the
// same slice for the same key until the next reload, as the Client contract asks.
func (c *ConfiguratorClient) GetValue(key Key) []ConstrainedValue {
	return (*c.values.Load())[key]
}

// LoadFile parses contents, evaluates every key against the ambient constraints, and
// installs the results. On any error the previous values are left in place.
func (c *ConfiguratorClient) LoadFile(contents []byte) error {
	var entries map[string]yamlExprEntry
	if err := yaml.Unmarshal(contents, &entries); err != nil {
		return fmt.Errorf("decoding expression config: %w", err)
	}

	// Reuse the evaluator's parsing, including validation against the settings registry.
	// The library resolves an index into a table of pre-decoded values; see exprSnapshot.
	stub := &ConfiguratorEvaluator{logger: c.logger}
	cfg := configurator.New[int]()
	parsed := make(map[Key]*exprEntry, len(entries))

	var errs []error
	for name, entry := range entries {
		key := MakeKey(name)
		e, blob, err := stub.parseEntry(key, entry)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := cfg.LoadKey(e.name, blob); err != nil {
			errs = append(errs, fmt.Errorf("key %q: %w", name, err))
			continue
		}
		parsed[key] = e
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// Evaluate once per key, now, against whatever this process knows about itself.
	values := make(ConfigValueMap, len(parsed))
	for key, e := range parsed {
		idx, err := cfg.Eval(context.Background(), e.name, c.ambient)
		if err != nil {
			errs = append(errs, fmt.Errorf("key %q: %w", key, err))
			continue
		}
		// A single value with no constraints: every precedence order ends in {}, so this
		// matches whatever the caller asks for.
		values[key] = []ConstrainedValue{{Value: e.outcomes[idx].Value}}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	old := c.values.Swap(&values)
	changed := make(map[Key][]ConstrainedValue)
	for key, cvs := range values {
		changed[key] = cvs
	}
	for key := range *old {
		if _, ok := values[key]; !ok {
			changed[key] = nil
		}
	}
	if len(changed) > 0 {
		c.PublishUpdates(changed)
	}
	c.logger.Info("Loaded expression config as dynamic config client",
		tag.NewInt("keys", len(values)))
	return nil
}
