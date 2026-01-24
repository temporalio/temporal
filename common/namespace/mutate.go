package namespace

import (
	namespacepb "go.temporal.io/api/namespace/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type mutationFunc func(*Namespace)

func (f mutationFunc) apply(ns *Namespace) {
	f(ns)
}

// WithActiveCluster assigns the active cluster to a Namespace during a Clone
// operation.
func WithActiveCluster(name string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.replicationResolver.SetActiveCluster(name)
		})
}

// WithBadBinary adds a bad binary checksum to a Namespace during a Clone
// operation.
func WithBadBinary(chksum string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			if ns.config.GetBadBinaries().GetBinaries() == nil {
				ns.config.GetBadBinaries().SetBinaries(make(map[string]*namespacepb.BadBinaryInfo))
			}
			ns.config.GetBadBinaries().GetBinaries()[chksum] =
				&namespacepb.BadBinaryInfo{}
		})
}

// WithID assigns the ID to a Namespace during a Clone operation.
func WithID(id string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.info.SetId(id)
		})
}

// WithGlobalFlag sets whether or not this Namespace is global.
func WithGlobalFlag(b bool) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.replicationResolver.SetGlobalFlag(b)
		})
}

// WithNotificationVersion assigns a notification version to the Namespace.
func WithNotificationVersion(v int64) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.notificationVersion = v
		})
}

// WithRetention assigns the retention duration to a Namespace during a Clone
// operation.
func WithRetention(dur *durationpb.Duration) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.config.SetRetention(dur)
		})
}

// WithData adds a key-value pair to a Namespace during a Clone operation.
func WithData(key, value string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			if ns.info.GetData() == nil {
				ns.info.SetData(make(map[string]string))
			}
			ns.info.GetData()[key] = value
		})
}

func WithPretendLocalNamespace(localClusterName string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.replicationResolver.PretendLocalNamespace(localClusterName)
		})
}
