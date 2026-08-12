package namespace

import (
	"time"

	namespacepb "go.temporal.io/api/namespace/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			if ns.config.BadBinaries.Binaries == nil {
				ns.config.BadBinaries.Binaries = make(map[string]*namespacepb.BadBinaryInfo)
			}
			ns.config.BadBinaries.Binaries[chksum] =
				&namespacepb.BadBinaryInfo{}
		})
}

// WithID assigns the ID to a Namespace during a Clone operation.
func WithID(id string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.info.Id = id
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
			ns.config.Retention = dur
		})
}

// WithClusterConnectTime sets the recorded connect time for cluster on a Namespace during a Clone
// operation, for use in tests exercising the namespace gradual-connect replication ramp (see
// Namespace.InitialConnectTime). Production code never sets this directly -- it's stamped by
// namespaceHandler.maybeUpdateClusterConnectTime as part of UpdateNamespace.
func WithClusterConnectTime(cluster string, connectTime time.Time) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			replicationConfig := ns.replicationResolver.ReplicationConfig()
			if replicationConfig.ClusterConnectTime == nil {
				replicationConfig.ClusterConnectTime = make(map[string]*timestamppb.Timestamp)
			}
			replicationConfig.ClusterConnectTime[cluster] = timestamppb.New(connectTime)
		})
}

// WithData adds a key-value pair to a Namespace during a Clone operation.
func WithData(key, value string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			if ns.info.Data == nil {
				ns.info.Data = make(map[string]string)
			}
			ns.info.Data[key] = value
		})
}

func WithPretendLocalNamespace(localClusterName string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.replicationResolver.PretendLocalNamespace(localClusterName)
		})
}
