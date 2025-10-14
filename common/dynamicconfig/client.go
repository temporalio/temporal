package dynamicconfig

import (
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
)

type (
	// Client is a source of dynamic configuration. The default Client, fileBasedClient, reads
	// from a file in the filesystem, and refreshes it periodically. You can extend the server
	// with an alternate Client using ServerOptions.
	Client interface {
		// GetValue returns a set of values and associated constraints for a key. Not all
		// constraints are valid for all keys.
		//
		// The returned slice of ConstrainedValues is treated as a set, and order does not
		// matter. The effective order of constraints is determined by server logic. See the
		// comment on Constraints below.
		//
		// If none of the ConstrainedValues match the constraints being used for the key, then
		// the server default value will be used.
		//
		// Note that GetValue is called very often! You should not synchronously call out to an
		// external system. Instead you should keep a set of all configured values, refresh it
		// periodically or when notified, and only do in-memory lookups inside of GetValue.
		//
		// Implementations should prefer to return the same slice in response to the same key
		// as long as the value hasn't changed. Value conversions are cached using weak
		// pointers into the returned slice, so new slices will result in unnecessary calls to
		// conversion functions.
		GetValue(key Key) []ConstrainedValue
	}

	// NotifyingClient is an optional interface that a Client can also implement, that adds
	// support for faster notifications of dynamic config changes.
	NotifyingClient interface {
		// Adds a subscription to all updates from this Client. `update` will be called on any
		// change to the current value set. The caller should call `cancel` to cancel the
		// subscription. Calls to `update` will not be made concurrently.
		Subscribe(update ClientUpdateFunc) (cancel func())
	}

	// Called with modified keys on any change to the current value set.
	// Deleted keys/constraints will get a nil value.
	ClientUpdateFunc func(map[Key][]ConstrainedValue)

	// Key is a key/property stored in dynamic config. For convenience, it is recommended that
	// you treat keys as case-insensitive.
	Key string

	// ConstrainedValue is a value plus associated constraints.
	//
	// The type of the Value field depends on the key. Acceptable types will be one of:
	//   int, float64, bool, string, map[string]any, time.Duration
	//
	// If time.Duration is expected, a string is also accepted, which will be converted using
	// timestamp.ParseDurationDefaultDays. If float64 is expected, int is also accepted. In
	// other cases, the exact type must be used. If a Value is returned with an unexpected
	// type, it will be ignored.
	ConstrainedValue struct {
		Constraints Constraints
		Value       any
	}
	TypedConstrainedValue[T any] struct {
		Constraints Constraints
		Value       T
	}

	// Constraints describe under what conditions a ConstrainedValue should be used.
	// There are few standard "constraint precedence orders" that the server uses:
	//   global precedence:
	//     no constraints
	//   namespace precedence:
	//     Namespace
	//     no constraints
	//   task queue precedence
	//     Namespace+TaskQueueName+TaskQueueType
	//     Namespace+TaskQueueName
	//     TaskQueueName
	//     Namespace
	//     no constraints
	//   shard id precedence:
	//     ShardID
	//     no constraints
	// In each case, the constraints that the server is checking and the constraints that apply
	// to the value must match exactly, including the fields that are not set (zero values).
	// That is, for keys that use namespace precedence, you must either return a
	// ConstrainedValue with only Namespace set, or with no fields set. (Or return one of
	// each.) If you return a ConstrainedValue with Namespace and ShardID set, for example,
	// that value will never be used, even if the Namespace matches.
	Constraints struct {
		Namespace     string
		NamespaceID   string
		TaskQueueName string
		TaskQueueType enumspb.TaskQueueType
		ShardID       int32
		TaskType      enumsspb.TaskType
		Destination   string
	}
)

func (k Key) String() string {
	return string(k)
}
