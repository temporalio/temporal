package interceptor

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
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
	req interface{},
) namespace.Name {
	namespaceName, err := GetNamespaceName(namespaceRegistry, req)
	if err != nil {
		return namespace.EmptyName
	}
	return namespaceName
}

func GetNamespaceName(
	namespaceRegistry namespace.Registry,
	req interface{},
) (namespace.Name, error) {
	switch request := req.(type) {
	case *workflowservice.RegisterNamespaceRequest:
		// For namespace registration requests, we don't expect to find namespace so skip checking caches
		// to avoid caching a NotFound error from persistence readthrough
		return namespace.Name(request.GetNamespace()), nil
	case NamespaceNameGetter:
		namespaceName := namespace.Name(request.GetNamespace())
		_, err := namespaceRegistry.GetNamespace(namespaceName)
		if err != nil {
			return namespace.EmptyName, err
		}
		return namespaceName, nil

	case NamespaceIDGetter:
		namespaceID := namespace.ID(request.GetNamespaceId())
		namespaceName, err := namespaceRegistry.GetNamespaceName(namespaceID)
		if err != nil {
			return namespace.EmptyName, err
		}
		return namespaceName, nil

	default:
		return namespace.EmptyName, serviceerror.NewInternalf("unable to extract namespace info from request of type %T", req)
	}
}
