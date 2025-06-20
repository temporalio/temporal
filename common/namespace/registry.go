//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination registry_mock.go

package namespace

import (
	"go.temporal.io/server/common/pingable"
)

type (
	// StateChangeCallbackFn can be registered to be called on any namespace state change or
	// addition/removal from database, plus once for all namespaces after registration. There
	// is no guarantee about when these are called.
	StateChangeCallbackFn func(ns *Namespace, deletedFromDb bool)

	// Registry provides access to Namespace objects by name or by ID.
	Registry interface {
		pingable.Pingable
		GetNamespace(name Name) (*Namespace, error)
		GetNamespaceWithOptions(name Name, opts GetNamespaceOptions) (*Namespace, error)
		GetNamespaceByID(id ID) (*Namespace, error)
		RefreshNamespaceById(namespaceId ID) (*Namespace, error)
		GetNamespaceByIDWithOptions(id ID, opts GetNamespaceOptions) (*Namespace, error)
		GetNamespaceID(name Name) (ID, error)
		GetNamespaceName(id ID) (Name, error)
		GetRegistrySize() (sizeOfCacheByName int64, sizeOfCacheByID int64)
		// Registers callback for namespace state changes.
		// StateChangeCallbackFn will be invoked for a new/deleted namespace or namespace that has
		// State, ReplicationState, ActiveCluster, or isGlobalNamespace config changed.
		RegisterStateChangeCallback(key any, cb StateChangeCallbackFn)
		UnregisterStateChangeCallback(key any)
		// GetCustomSearchAttributesMapper is a temporary solution to be able to get search attributes
		// with from persistence if forceSearchAttributesCacheRefreshOnRead is true.
		GetCustomSearchAttributesMapper(name Name) (CustomSearchAttributesMapper, error)
		Start()
		Stop()
	}

	GetNamespaceOptions struct {
		// Setting this disables the readthrough logic, i.e. only looks at the current in-memory
		// registry. This is useful if you want to avoid latency or avoid polluting the negative
		// lookup cache. Note that you may get false negatives (namespace not found) if the
		// namespace was created very recently.
		DisableReadthrough bool
	}
)
