package nsreplication

import (
	"context"
)

// RepairPolicy decides whether operator repair may change a namespace. Repair
// assumes ReplicationConfig describes the namespace's replicas; a deployment
// that keeps replica membership elsewhere can refuse the namespaces repair does
// not support yet. It is checked against the namespace that verify reads, the
// repair payload, and the stored namespace that a repair would update. The
// returned error reaches the operator unchanged.
type RepairPolicy func(ctx context.Context, namespaceID, namespaceName string, data map[string]string) error

// NewDefaultRepairPolicy creates the default RepairPolicy, which allows every namespace.
func NewDefaultRepairPolicy() RepairPolicy {
	return func(context.Context, string, string, map[string]string) error { return nil }
}
