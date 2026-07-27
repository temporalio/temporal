package configurator

import (
	"encoding/json"
	"fmt"
)

// jsonConfig is the JSON form of a Config: values are raw, to be decoded into T.
type jsonConfig struct {
	DefaultValue json.RawMessage
	Overrides    []struct {
		MatchString string
		MatchResult json.RawMessage
	}
}

// JSONConfig decodes a Config[T] whose values are JSON, for callers that want the library to
// own value decoding.
//
// This is a convenience layer, not part of the core: Configurator itself treats values as
// opaque. Decoding here goes through encoding/json, so the usual caveats apply — in
// particular every JSON number decodes to float64 when T is `any`, which will not satisfy a
// caller expecting an int. A caller with its own notion of types should decode values itself
// and build a Config[V] directly.
func JSONConfig[T any](data []byte) (Config[T], error) {
	var raw jsonConfig
	if err := json.Unmarshal(data, &raw); err != nil {
		return Config[T]{}, fmt.Errorf("decoding config: %w", err)
	}

	var cfg Config[T]
	if len(raw.DefaultValue) > 0 {
		if err := json.Unmarshal(raw.DefaultValue, &cfg.DefaultValue); err != nil {
			return Config[T]{}, fmt.Errorf("decoding default value: %w", err)
		}
	}
	for _, o := range raw.Overrides {
		var v T
		if err := json.Unmarshal(o.MatchResult, &v); err != nil {
			return Config[T]{}, fmt.Errorf("decoding match result for %q: %w", o.MatchString, err)
		}
		cfg.Overrides = append(cfg.Overrides, Override[T]{MatchString: o.MatchString, MatchResult: v})
	}
	return cfg, nil
}

// LoadJSON is JSONConfig followed by Load, for callers migrating from the old byte-oriented
// entry point.
func LoadJSON[T any](c Configurator[T], configKey string, data []byte) error {
	cfg, err := JSONConfig[T](data)
	if err != nil {
		return fmt.Errorf("couldn't load %q: %w", configKey, err)
	}
	return c.Load(configKey, cfg)
}
