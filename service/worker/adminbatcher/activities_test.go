package adminbatcher

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

const (
	userNSName  = "user-ns"
	userNSID    = "user-ns-id"
	otherNSName = "other-ns"
)

func TestValidateAndResolveNSForAdminBatch(t *testing.T) {
	t.Run("resolves the namespace id from the registry", func(t *testing.T) {
		resolver := newResolver(t, func(r *namespace.MockRegistry) {
			r.EXPECT().GetNamespaceName(namespace.ID(userNSID)).Return(namespace.Name(userNSName), nil)
		})
		ns, err := resolver(&batchspb.BatchOperationInput{
			NamespaceId:  userNSID,
			AdminRequest: &adminservice.StartAdminBatchOperationRequest{Namespace: userNSName},
		})
		require.NoError(t, err)
		require.Equal(t, namespace.Name(userNSName), ns)
	})

	t.Run("rejects a name that does not match the id", func(t *testing.T) {
		resolver := newResolver(t, func(r *namespace.MockRegistry) {
			r.EXPECT().GetNamespaceName(namespace.ID(userNSID)).Return(namespace.Name(userNSName), nil)
		})
		_, err := resolver(&batchspb.BatchOperationInput{
			NamespaceId:  userNSID,
			AdminRequest: &adminservice.StartAdminBatchOperationRequest{Namespace: otherNSName},
		})
		require.ErrorIs(t, err, errNamespaceMismatch)
	})

	t.Run("rejects a non-admin batch", func(t *testing.T) {
		resolver := newResolver(t, nil)
		_, err := resolver(&batchspb.BatchOperationInput{
			NamespaceId: userNSID,
			Request:     &workflowservice.StartBatchOperationRequest{Namespace: userNSName},
		})
		require.ErrorIs(t, err, errNonAdminBatch)
	})

	t.Run("rejects a batch targeting the system namespace", func(t *testing.T) {
		resolver := newResolver(t, func(r *namespace.MockRegistry) {
			r.EXPECT().GetNamespaceName(namespace.ID(primitives.SystemNamespaceID)).
				Return(namespace.Name(primitives.SystemLocalNamespace), nil)
		})
		_, err := resolver(&batchspb.BatchOperationInput{
			NamespaceId: primitives.SystemNamespaceID,
			AdminRequest: &adminservice.StartAdminBatchOperationRequest{
				Namespace: primitives.SystemLocalNamespace,
			},
		})
		require.ErrorIs(t, err, errNonUserNamespace)
	})

	t.Run("propagates a registry lookup failure", func(t *testing.T) {
		notFound := serviceerror.NewNamespaceNotFound(userNSID)
		resolver := newResolver(t, func(r *namespace.MockRegistry) {
			r.EXPECT().GetNamespaceName(namespace.ID(userNSID)).Return(namespace.EmptyName, notFound)
		})
		_, err := resolver(&batchspb.BatchOperationInput{
			NamespaceId:  userNSID,
			AdminRequest: &adminservice.StartAdminBatchOperationRequest{Namespace: userNSName},
		})
		require.ErrorIs(t, err, notFound)
	})
}

func newResolver(t *testing.T, setup func(*namespace.MockRegistry)) func(*batchspb.BatchOperationInput) (namespace.Name, error) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	if setup != nil {
		setup(registry)
	}
	return validateAndResolveNSForAdminBatch(registry)
}
