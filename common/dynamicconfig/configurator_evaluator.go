package dynamicconfig

import (
	"context"
	"maps"
	"os"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/dynamicconfig/configurator"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// AmbientConstraints describes the process-scoped constraint dimensions that every
// expression evaluation can match on, regardless of which setting is being read.
//
// These are exactly the dimensions that the Constraints struct cannot express: adding one
// today would mean a new field, a new precedence order in cmd/tools/gendynamicconfig, and a
// regeneration of setting_gen.go.
type AmbientConstraints struct {
	// Environment, e.g. "production" or "staging".
	Environment string
	// AvailabilityZone or region, e.g. "us-west-2".
	AvailabilityZone string
	// ClusterName as configured in clusterMetadata.
	ClusterName string
	// ServiceName, e.g. "frontend", "history", "matching", "worker".
	ServiceName string
	// Custom carries operator-defined dimensions verbatim. Keys here do not collide with the
	// built-in ones above or with any precedence-derived key.
	Custom map[string]any
}

// asMap flattens the ambient constraints once, at construction time.
func (a AmbientConstraints) asMap() map[string]any {
	m := make(map[string]any, len(a.Custom)+6)
	maps.Copy(m, a.Custom)
	putNonEmpty(m, "env", a.Environment)
	putNonEmpty(m, "zone", a.AvailabilityZone)
	putNonEmpty(m, "cluster", a.ClusterName)
	putNonEmpty(m, "service", a.ServiceName)
	putNonEmpty(m, "serverVersion", headers.ServerVersion)
	if host, err := os.Hostname(); err == nil {
		putNonEmpty(m, "host", host)
	}
	return m
}

type (
	// ConfiguratorEvaluator is an Evaluator backed by the configurator expression library.
	// See common/dynamicconfig/configurator/README.md.
	ConfiguratorEvaluator struct {
		logger  log.Logger
		ambient map[string]any

		// subscribers are invoked after each reload with the keys whose configuration
		// changed. There is one per Collection, and a single binary running several
		// services has several Collections, so this cannot be a single callback slot.
		subscriberLock sync.Mutex
		subscribers    map[int]func([]Key)
		subscriberIdx  int

		// snapshot is replaced wholesale on reload. It is never mutated in place, both
		// because readers are lock-free and because the underlying library's LoadKey is not
		// safe to call concurrently with Eval.
		snapshot atomic.Pointer[exprSnapshot]

		errCount atomic.Int64
	}

	// exprSnapshot is one immutable generation of the expression configuration.
	exprSnapshot struct {
		// cfg resolves a key to an *index* into the corresponding entry's outcomes rather
		// than to the value itself. That keeps the returned *ConstrainedValue pointers
		// stable for the lifetime of the snapshot, which is what lets Collection cache
		// conversions against them with weak pointers, and it means Eval never decodes.
		cfg  configurator.Configurator[int]
		keys map[Key]*exprEntry
	}

	exprEntry struct {
		// name is the key as the library knows it (the lower-cased Key string).
		name string
		// outcomes[0] is the default; outcomes[i+1] is override i's result, in file order.
		outcomes []*ConstrainedValue
		// fingerprint is the JSON of the source YAML entry, used to detect changes across
		// reloads so that only genuinely changed keys notify subscribers.
		fingerprint []byte
	}
)

var _ Evaluator = (*ConfiguratorEvaluator)(nil)

// NewConfiguratorEvaluator returns an evaluator with no configuration loaded. Every key
// reports Has() == false until LoadFile succeeds, so an evaluator that never loads is
// equivalent to having none.
func NewConfiguratorEvaluator(ambient AmbientConstraints, logger log.Logger) *ConfiguratorEvaluator {
	e := &ConfiguratorEvaluator{
		logger:      logger,
		ambient:     ambient.asMap(),
		subscribers: make(map[int]func([]Key)),
	}
	e.errCount.Store(-1)
	return e
}

// Subscribe registers a callback invoked with the set of keys whose expression configuration
// changed, after each successful reload. Call cancel to unsubscribe.
//
// This is normally wired to Collection.EvaluatorKeysChanged so that settings subscribers see
// expression config updates the same way they see file config updates.
func (e *ConfiguratorEvaluator) Subscribe(f func([]Key)) (cancel func()) {
	e.subscriberLock.Lock()
	defer e.subscriberLock.Unlock()

	e.subscriberIdx++
	id := e.subscriberIdx
	e.subscribers[id] = f

	return func() {
		e.subscriberLock.Lock()
		defer e.subscriberLock.Unlock()
		delete(e.subscribers, id)
	}
}

func (e *ConfiguratorEvaluator) publish(changed []Key) {
	e.subscriberLock.Lock()
	defer e.subscriberLock.Unlock()
	for _, f := range e.subscribers {
		f(changed)
	}
}

// Has implements Evaluator. It is on the read path of every setting, so it does no more than
// an atomic load and a map lookup.
func (e *ConfiguratorEvaluator) Has(key Key) bool {
	snap := e.snapshot.Load()
	if snap == nil {
		return false
	}
	_, ok := snap.keys[key]
	return ok
}

// Eval implements Evaluator.
func (e *ConfiguratorEvaluator) Eval(key Key, base Constraints, extra map[string]any) *ConstrainedValue {
	snap := e.snapshot.Load()
	if snap == nil {
		return nil
	}
	entry, ok := snap.keys[key]
	if !ok {
		return nil
	}

	// The library only reads the constraint map during Eval and never retains it, so the map
	// can be recycled. Building it fresh each read is otherwise the dominant cost of an
	// expression backed lookup.
	constraints := constraintMapPool.Get().(map[string]any) //nolint:revive // unchecked-type-assertion
	e.fillConstraints(constraints, base, extra)
	idx, err := snap.cfg.Eval(context.Background(), entry.name, constraints)
	clear(constraints)
	constraintMapPool.Put(constraints)

	if err != nil {
		if e.throttleLog() {
			e.logger.Warn("Failed to evaluate expression config, falling back to file config",
				tag.Key(key.String()), tag.Error(err))
		}
		return nil
	}
	if idx < 0 || idx >= len(entry.outcomes) {
		// Not reachable: indexes are generated alongside outcomes in loadExpressionFile.
		if e.throttleLog() {
			e.logger.Warn("Expression config produced an out-of-range outcome (this is a bug)",
				tag.Key(key.String()), tag.NewInt("index", idx))
		}
		return nil
	}
	return entry.outcomes[idx]
}

// constraintMapPool recycles the maps handed to the library on each evaluation.
var constraintMapPool = sync.Pool{
	New: func() any { return make(map[string]any, 16) },
}

// fillConstraints populates c with the constraints for one evaluation, from three sources in
// increasing order of specificity: process-ambient dimensions, the dimensions implied by the
// setting's precedence, and any ad-hoc dimensions attached with Collection.WithConstraints.
//
// Zero-valued precedence fields are omitted rather than inserted as empty strings, so an
// expression that tests them simply fails to match, the same way an unconstrained lookup
// cannot match a constrained value today.
func (e *ConfiguratorEvaluator) fillConstraints(c map[string]any, base Constraints, extra map[string]any) {
	maps.Copy(c, e.ambient)

	putNonEmpty(c, "namespace", base.Namespace)
	putNonEmpty(c, "namespaceID", base.NamespaceID)
	putNonEmpty(c, "taskQueueName", base.TaskQueueName)
	putNonEmpty(c, "destination", base.Destination)
	putNonEmpty(c, "chasmTaskType", base.ChasmTaskType)
	if base.TaskQueueType != 0 {
		c["taskQueueType"] = base.TaskQueueType.String()
	}
	if base.TaskType != 0 {
		c["historyTaskType"] = base.TaskType.String()
	}
	if base.ShardID != 0 {
		c["shardID"] = int(base.ShardID)
	}

	maps.Copy(c, extra)
}

func (e *ConfiguratorEvaluator) throttleLog() bool {
	n := e.errCount.Add(1)
	return n < errCountLogThreshold || n%errCountLogThreshold == 0
}

func putNonEmpty(m map[string]any, key, value string) {
	if value != "" {
		m[key] = value
	}
}
