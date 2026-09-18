package nsreplication

import (
	"crypto/sha256"
	"slices"

	replicationpb "go.temporal.io/api/replication/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"google.golang.org/protobuf/proto"
)

// NamespaceTaskFingerprint returns a stable fingerprint of the receiver wire payload.
func NamespaceTaskFingerprint(task *replicationspb.NamespaceTaskAttributes) ([]byte, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(task)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(payload)
	return fingerprint[:], nil
}

// DifferingNamespaceTaskFields reports the top-level wire fields that differ.
func DifferingNamespaceTaskFields(
	a *replicationspb.NamespaceTaskAttributes,
	b *replicationspb.NamespaceTaskAttributes,
) []string {
	var fields []string
	if a.GetNamespaceOperation() != b.GetNamespaceOperation() {
		fields = append(fields, "namespace_operation")
	}
	if a.GetId() != b.GetId() {
		fields = append(fields, "id")
	}
	if !proto.Equal(a.GetInfo(), b.GetInfo()) {
		fields = append(fields, "info")
	}
	if !proto.Equal(a.GetConfig(), b.GetConfig()) {
		fields = append(fields, "config")
	}
	if !proto.Equal(a.GetReplicationConfig(), b.GetReplicationConfig()) {
		fields = append(fields, "replication_config")
	}
	if a.GetConfigVersion() != b.GetConfigVersion() {
		fields = append(fields, "config_version")
	}
	if a.GetFailoverVersion() != b.GetFailoverVersion() {
		fields = append(fields, "failover_version")
	}
	if !slices.EqualFunc(a.GetFailoverHistory(), b.GetFailoverHistory(), func(a, b *replicationpb.FailoverStatus) bool {
		return proto.Equal(a, b)
	}) {
		fields = append(fields, "failover_history")
	}
	return fields
}
