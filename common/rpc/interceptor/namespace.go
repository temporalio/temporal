package interceptor

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
)

// gRPC method request must implement either NamespaceNameGetter or NamespaceIDGetter
// for namespace specific metrics to be reported properly
type (
	NamespaceNameGetter interface {
		GetNamespace() string
	}

	NamespaceIDGetter interface {
		GetNamespaceId() string
	}
)

// MustGetNamespaceName returns request namespace name
// or EmptyName if there's error when retrieving namespace name,
// e.g. unable to find namespace
func MustGetNamespaceName(
	namespaceRegistry namespace.Registry,
	req any,
) namespace.Name {
	namespaceName, err := GetNamespaceName(namespaceRegistry, req)
	if err != nil {
		return namespace.EmptyName
	}
	return namespaceName
}

func GetNamespaceName(
	namespaceRegistry namespace.Registry,
	req any,
) (namespace.Name, error) {
	switch request := req.(type) {
	case *workflowservice.RegisterNamespaceRequest:
		// For namespace registration requests, we don't expect to find namespace so skip checking caches
		// to avoid caching a NotFound error from persistence readthrough
		return namespace.Name(request.GetNamespace()), nil
	case *historyservice.CompleteNexusOperationRequest:
		return getNamespaceNameByID(namespaceRegistry, request.GetCompletion().GetNamespaceId())
	case *historyservice.CompleteNexusOperationChasmRequest:
		// The namespace is inside the serialized component ref, which the history client also decodes to route
		// the request.
		ref := &persistencespb.ChasmComponentRef{}
		if err := ref.Unmarshal(request.GetCompletion().GetComponentRef()); err != nil {
			return namespace.EmptyName, serviceerror.NewInvalidArgument("unable to extract namespace from component ref")
		}
		return getNamespaceNameByID(namespaceRegistry, ref.GetNamespaceId())
	case NamespaceNameGetter:
		namespaceName := namespace.Name(request.GetNamespace())
		_, err := namespaceRegistry.GetNamespace(namespaceName)
		if err != nil {
			return namespace.EmptyName, err
		}
		return namespaceName, nil

	case NamespaceIDGetter:
		return getNamespaceNameByID(namespaceRegistry, request.GetNamespaceId())

	default:
		return namespace.EmptyName, serviceerror.NewInternalf("unable to extract namespace info from request of type %T", req)
	}
}

func getNamespaceNameByID(namespaceRegistry namespace.Registry, namespaceID string) (namespace.Name, error) {
	namespaceName, err := namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return namespace.EmptyName, err
	}
	return namespaceName, nil
}
