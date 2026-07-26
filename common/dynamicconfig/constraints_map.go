package dynamicconfig

import (
	"context"

	"github.com/blang/semver/v4"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
)

// Built-in constraint keys. Expressions may also use any key declared in the static config's
// expressionConstraints, or supplied ad hoc by a caller; see the vocabulary check in
// configurator_client.go.
const (
	// Supplied by the process, merged into every evaluation.
	CKEnvironment   = "env"
	CKZone          = "zone"
	CKCluster       = "cluster"
	CKService       = "service"
	CKHost          = "host"
	CKServerVersion = "serverVersion"

	// Supplied by the caller.
	CKNamespace     = "namespace"
	CKNamespaceID   = "namespaceID"
	CKTaskQueueName = "taskQueueName"
	CKTaskQueueType = "taskQueueType"
	CKDestination   = "destination"
	CKChasmTaskType = "chasmTaskType"
	CKShardID       = "shardID"
	CKTaskType      = "historyTaskType"

	// Derived from the request context by ConstraintsFromContext.
	CKSDKName    = "sdkName"
	CKSDKVersion = "sdkVersion"
	CKSDKMajor   = "sdkMajor"
	CKSDKMinor   = "sdkMinor"
	CKSDKPatch   = "sdkPatch"
	CKCallerName = "callerName"
	CKCallerType = "callerType"
	CKCallOrigin = "callOrigin"
)

// builtinConstraintKeys is the vocabulary an expression may draw on without declaring
// anything. See ConfiguratorClient's vocabulary check, which rejects unknown keys at load so
// that a typo fails loudly instead of silently resolving to the default.
var builtinConstraintKeys = map[string]struct{}{
	CKEnvironment: {}, CKZone: {}, CKCluster: {}, CKService: {}, CKHost: {}, CKServerVersion: {},
	CKNamespace: {}, CKNamespaceID: {}, CKTaskQueueName: {}, CKTaskQueueType: {},
	CKDestination: {}, CKChasmTaskType: {}, CKShardID: {}, CKTaskType: {},
	CKSDKName: {}, CKSDKVersion: {}, CKSDKMajor: {}, CKSDKMinor: {}, CKSDKPatch: {},
	CKCallerName: {}, CKCallerType: {}, CKCallOrigin: {},
}

// ConstraintsMap is the open set of dimensions a caller supplies to a dynamic config lookup.
//
// It is deliberately not the same thing as Constraints: Constraints describes what a *stored
// value* matches and is a closed struct, whereas a ConstraintsMap describes what the *caller*
// knows and can hold anything an expression might want to test.
//
// Process-scoped dimensions (env, zone, cluster, service, host, serverVersion) are held by
// the evaluator and merged into every evaluation, so they do not belong here and a caller
// that supplies nothing at all still gets them.
//
// Build one per request or per component and reuse it across lookups rather than building one
// per lookup.
type ConstraintsMap map[string]any

// Get implements the expression library's Lookup interface.
func (c ConstraintsMap) Get(key string) (any, bool) {
	v, ok := c[key]
	return v, ok
}

// With adds a dimension and returns the map so calls can be chained.
//
// It mutates in place when the receiver is non-nil, so the return value can be ignored in
// that case. On a nil receiver it has to allocate, so always use the result if the map may be
// nil — prefer starting from one of the constructors below.
func (c ConstraintsMap) With(key string, value any) ConstraintsMap {
	if c == nil {
		c = make(ConstraintsMap, 4)
	}
	c[key] = value
	return c
}

// WithNS is shorthand for With(CKNamespace, ns.String()).
func (c ConstraintsMap) WithNS(ns namespace.Name) ConstraintsMap {
	return c.With(CKNamespace, ns.String())
}

// NewConstraintsMap returns an empty map ready for With.
func NewConstraintsMap() ConstraintsMap {
	return make(ConstraintsMap, 4)
}

// ConstraintsWithNS is shorthand for the common case of a namespace-scoped lookup.
func ConstraintsWithNS(ns namespace.Name) ConstraintsMap {
	return ConstraintsMap{CKNamespace: ns.String()}
}

// --- projection onto the legacy Constraints struct ---------------------------------------
//
// A call site that has moved to GetC must still honour constrained values in the dynamic
// config file, so the known keys are projected back out of the map into the precedence list
// the Client lookup expects. These disappear along with the precedence system.

func (c ConstraintsMap) str(key string) string {
	s, _ := c[key].(string) //nolint:revive // a non-string is treated as absent
	return s
}

func (c ConstraintsMap) shardID() int32 {
	switch v := c[CKShardID].(type) {
	case int32:
		return v
	case int:
		return int32(v) //nolint:gosec // shard ids are small
	default:
		return 0
	}
}

// taskQueueType accepts the enum or, as YAML and ambient constraints carry it, its name.
func (c ConstraintsMap) taskQueueType() enumspb.TaskQueueType {
	switch v := c[CKTaskQueueType].(type) {
	case enumspb.TaskQueueType:
		return v
	case string:
		if t, err := enumspb.TaskQueueTypeFromString(v); err == nil {
			return t
		}
		return enumspb.TASK_QUEUE_TYPE_UNSPECIFIED
	default:
		return enumspb.TASK_QUEUE_TYPE_UNSPECIFIED
	}
}

// historyTaskType accepts the enum or its name, as taskQueueType does.
func (c ConstraintsMap) historyTaskType() enumsspb.TaskType {
	switch v := c[CKTaskType].(type) {
	case enumsspb.TaskType:
		return v
	case string:
		if t, err := enumsspb.TaskTypeFromString(v); err == nil {
			return t
		}
		return enumsspb.TASK_TYPE_UNSPECIFIED
	default:
		return enumsspb.TASK_TYPE_UNSPECIFIED
	}
}

func (c ConstraintsMap) globalPrecedence() []Constraints {
	return []Constraints{{}}
}

func (c ConstraintsMap) namespacePrecedence() []Constraints {
	return []Constraints{{Namespace: c.str(CKNamespace)}, {}}
}

func (c ConstraintsMap) namespaceIDPrecedence() []Constraints {
	return []Constraints{{NamespaceID: c.str(CKNamespaceID)}, {}}
}

func (c ConstraintsMap) taskQueuePrecedence() []Constraints {
	ns, tq, tqType := c.str(CKNamespace), c.str(CKTaskQueueName), c.taskQueueType()
	return []Constraints{
		{Namespace: ns, TaskQueueName: tq, TaskQueueType: tqType},
		{Namespace: ns, TaskQueueName: tq},
		{TaskQueueName: tq},
		{Namespace: ns},
		{},
	}
}

func (c ConstraintsMap) shardIDPrecedence() []Constraints {
	return []Constraints{{ShardID: c.shardID()}, {}}
}

func (c ConstraintsMap) taskTypePrecedence() []Constraints {
	return []Constraints{{TaskType: c.historyTaskType()}, {}}
}

func (c ConstraintsMap) destinationPrecedence() []Constraints {
	ns, dest := c.str(CKNamespace), c.str(CKDestination)
	return []Constraints{
		{Namespace: ns, Destination: dest},
		{Destination: dest},
		{Namespace: ns},
		{},
	}
}

func (c ConstraintsMap) chasmTaskTypePrecedence() []Constraints {
	return []Constraints{{ChasmTaskType: c.str(CKChasmTaskType)}, {}}
}

// ConstraintsFromContext extracts the request-scoped dimensions carried in ctx: the calling
// SDK's name and version, and the caller info. These propagate from the frontend to history
// and matching over gRPC metadata (common/rpc/grpc.go), so they are available server-side.
//
// The SDK version is supplied both as the raw string and as separate numeric components,
// because the expression DSL compares strings lexicographically — "1.9.0" sorts *after*
// "1.28.0" — so any ordered comparison has to be on the numeric parts.
//
// Returns an empty (non-nil) map when ctx carries nothing, so it is always safe to chain.
func ConstraintsFromContext(ctx context.Context) ConstraintsMap {
	c := make(ConstraintsMap, 8)

	if name, version := headers.GetClientNameAndVersion(ctx); name != "" || version != "" {
		if name != "" {
			c[CKSDKName] = name
		}
		if version != "" {
			c[CKSDKVersion] = version
			if v, err := semver.Parse(version); err == nil {
				c[CKSDKMajor] = int(v.Major)
				c[CKSDKMinor] = int(v.Minor)
				c[CKSDKPatch] = int(v.Patch)
			}
		}
	}

	if info := headers.GetCallerInfo(ctx); info != (headers.CallerInfo{}) {
		if info.CallerName != "" {
			c[CKCallerName] = info.CallerName
		}
		if info.CallerType != "" {
			c[CKCallerType] = info.CallerType
		}
		if info.CallOrigin != "" {
			c[CKCallOrigin] = info.CallOrigin
		}
	}

	return c
}
