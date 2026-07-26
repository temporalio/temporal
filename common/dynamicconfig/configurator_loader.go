package dynamicconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"go.temporal.io/server/common/dynamicconfig/configurator"
	"go.temporal.io/server/common/dynamicconfig/configurator/types"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/util"
	"gopkg.in/yaml.v3"
)

// ExpressionFilePollInterval is how often the expression config file is stat'd for changes.
// It matches the minimum poll interval of the file based client.
const ExpressionFilePollInterval = 5 * time.Second

// yamlExprEntry is the on-disk form of one setting's expression configuration. It matches
// the schema of the upstream configurator library:
//
//	history.transferProcessorMaxPollInterval:
//	  defaultValue: 1m
//	  overrides:
//	    - matchString: '"zone" = "us-west-2" and "namespace" = "canary"'
//	      matchResult: 10s
type yamlExprEntry struct {
	DefaultValue any `yaml:"defaultValue" json:"defaultValue"`
	Overrides    []struct {
		MatchString string `yaml:"matchString" json:"matchString"`
		MatchResult any    `yaml:"matchResult" json:"matchResult"`
	} `yaml:"overrides" json:"overrides"`
}

// LoadFile parses contents and, if it is valid, atomically installs it as the evaluator's
// configuration. On any error the previous configuration is left in place, so a bad edit
// degrades to the last good state rather than to compiled-in defaults.
//
// Warnings (unregistered keys, values that fail the setting's own validation) are logged but
// do not abort the load, matching the behaviour of the yaml loader used by the file based
// client.
func (e *ConfiguratorEvaluator) LoadFile(contents []byte) error {
	var entries map[string]yamlExprEntry
	if err := yaml.Unmarshal(contents, &entries); err != nil {
		return fmt.Errorf("decoding expression config: %w", err)
	}

	snap := &exprSnapshot{
		cfg:  configurator.New[int](),
		keys: make(map[Key]*exprEntry, len(entries)),
	}

	var errs []error
	for name, entry := range entries {
		key := MakeKey(name)
		parsed, libraryCfg, err := e.parseEntry(key, entry)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := snap.cfg.LoadKey(parsed.name, libraryCfg); err != nil {
			errs = append(errs, fmt.Errorf("key %q: %w", name, err))
			continue
		}
		snap.keys[key] = parsed
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	old := e.snapshot.Swap(snap)
	if changed := changedKeys(old, snap); len(changed) > 0 {
		e.publish(changed)
	}
	return nil
}

// parseEntry converts one YAML entry into an exprEntry plus the JSON blob handed to the
// library. The library config carries outcome *indexes* rather than values; see the comment
// on exprSnapshot.
func (e *ConfiguratorEvaluator) parseEntry(key Key, entry yamlExprEntry) (*exprEntry, []byte, error) {
	setting := queryRegistry(key)
	if setting == nil {
		e.logger.Warn("Expression config contains unregistered dynamic config key",
			tag.Key(key.String()))
	}

	outcome := func(v any, what string) (*ConstrainedValue, error) {
		// yaml decodes nested maps as map[any]any; dynamic config values must use string keys.
		converted, err := convertKeyTypeToString(v)
		if err != nil {
			return nil, fmt.Errorf("key %q %s: %w", key, what, err)
		}
		if setting != nil {
			if valErr := setting.Validate(converted); valErr != nil {
				return nil, fmt.Errorf("key %q %s: %w", key, what, valErr)
			}
		}
		return &ConstrainedValue{Value: converted}, nil
	}

	def, err := outcome(entry.DefaultValue, "defaultValue")
	if err != nil {
		return nil, nil, err
	}

	parsed := &exprEntry{
		name:     key.String(),
		outcomes: make([]*ConstrainedValue, 0, len(entry.Overrides)+1),
	}
	parsed.outcomes = append(parsed.outcomes, def)

	libraryCfg := types.Config{DefaultValue: json.RawMessage("0")}
	for i, o := range entry.Overrides {
		cv, err := outcome(o.MatchResult, fmt.Sprintf("override %d (%q)", i, o.MatchString))
		if err != nil {
			return nil, nil, err
		}
		parsed.outcomes = append(parsed.outcomes, cv)
		libraryCfg.Overrides = append(libraryCfg.Overrides, types.Override{
			MatchString: o.MatchString,
			// index into parsed.outcomes; 0 is the default, so overrides start at 1
			MatchResult: json.RawMessage(strconv.Itoa(len(parsed.outcomes) - 1)),
		})
	}

	// Recorded so reloads can tell which keys actually changed. Uses the source values, not
	// the indexes, so a changed matchResult is detected.
	if parsed.fingerprint, err = json.Marshal(entry); err != nil {
		return nil, nil, fmt.Errorf("key %q: %w", key, err)
	}

	blob, err := json.Marshal(libraryCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("key %q: %w", key, err)
	}
	// LoadKey also parses every matchString, so a malformed expression fails here rather
	// than silently never matching at runtime.
	return parsed, blob, nil
}

// changedKeys returns the keys whose configuration differs between two snapshots, including
// keys added and keys removed.
func changedKeys(prev, next *exprSnapshot) []Key {
	var changed []Key
	for key, entry := range next.keys {
		if prev == nil {
			changed = append(changed, key)
			continue
		}
		before, ok := prev.keys[key]
		if !ok || !bytes.Equal(before.fingerprint, entry.fingerprint) {
			changed = append(changed, key)
		}
	}
	if prev != nil {
		for key := range prev.keys {
			if _, ok := next.keys[key]; !ok {
				changed = append(changed, key)
			}
		}
	}
	slices.SortFunc(changed, func(a, b Key) int { return cmpKeys(a, b) })
	return changed
}

func cmpKeys(a, b Key) int {
	switch as, bs := a.String(), b.String(); {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

// LoadFileFrom reads path and loads it.
func (e *ConfiguratorEvaluator) LoadFileFrom(path string) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading expression config %s: %w", path, err)
	}
	return e.LoadFile(contents)
}

// Watch polls path and reloads it whenever its modification time advances past lastMod. It
// returns when ctx is cancelled. Reload failures are logged and retried on the next change.
//
// Callers normally get lastMod from StartWatching, which stats the file synchronously so
// that an edit landing between the initial load and the first poll is not missed.
func (e *ConfiguratorEvaluator) Watch(ctx context.Context, path string, interval time.Duration, lastMod time.Time) error {
	for ctx.Err() == nil {
		_ = util.InterruptibleSleep(ctx, interval)
		info, err := os.Stat(path)
		if err != nil {
			e.logger.Warn("Failed to stat expression config", tag.NewStringTag("path", path), tag.Error(err))
			continue
		}
		if !info.ModTime().After(lastMod) {
			continue
		}
		lastMod = info.ModTime()
		if err := e.LoadFileFrom(path); err != nil {
			e.logger.Error("Failed to reload expression config, keeping previous values",
				tag.NewStringTag("path", path), tag.Error(err))
		} else {
			e.logger.Info("Reloaded expression config", tag.NewStringTag("path", path))
		}
	}
	return ctx.Err()
}

// StartWatching begins watching path in the background. The returned func stops it and is
// safe to call more than once.
func (e *ConfiguratorEvaluator) StartWatching(path string, interval time.Duration) func() {
	// Stat before returning, so that the baseline is the file as it was at the caller's
	// initial load rather than whenever the goroutine happens to be scheduled.
	var lastMod time.Time
	if info, err := os.Stat(path); err == nil {
		lastMod = info.ModTime()
	}

	var g goro.Group
	g.Go(func(ctx context.Context) error {
		return e.Watch(ctx, path, interval, lastMod)
	})

	var once sync.Once
	return func() {
		once.Do(func() {
			g.Cancel()
			g.Wait()
		})
	}
}
