package adminbatcher

import (
	"errors"

	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
)

var (
	errNonAdminBatch     = errors.New("only admin batch operations may run on the system worker")
	errNamespaceMismatch = errors.New("namespace mismatch")
	errNonUserNamespace  = errors.New("admin batch must target a user namespace")
)

// the activity forever runs in system namespace
func validateAndResolveNSForAdminBatch(registry namespace.Registry) func(*batchspb.BatchOperationInput) (namespace.Name, error) {
	return func(batchParams *batchspb.BatchOperationInput) (namespace.Name, error) {
		adminReq := batchParams.GetAdminRequest()
		if adminReq == nil {
			return "", errNonAdminBatch
		}
		ns, err := registry.GetNamespaceName(namespace.ID(batchParams.GetNamespaceId()))
		if err != nil {
			return "", err
		}
		if ns.String() != adminReq.GetNamespace() {
			return "", errNamespaceMismatch
		}
		if ns.String() == primitives.SystemLocalNamespace {
			return "", errNonUserNamespace
		}
		return ns, nil
	}
}
