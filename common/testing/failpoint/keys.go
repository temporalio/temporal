package failpoint

import (
	"fmt"
	"sync"

	"go.temporal.io/server/common/namespace"
)

// Key identifies a failpoint by name. A key applies to all namespaces unless it is scoped to one
// with ForNamespace.
type Key struct {
	name        string
	namespaceID namespace.ID
}

// Failpoints in server code. Each key documents where it fires and what tests use it for.
var (
	// MatchingForwardTask fires before matching forwards a workflow, activity, query or Nexus task
	// to the parent partition. Tests use Pause to make forwarding latency observable.
	MatchingForwardTask = NewKey("matching.forwardTask")
	// UpdateWithStartBetweenLockAndStart fires after Update-with-Start released the workflow lock
	// and before it starts the workflow. Tests use Call to start a conflicting workflow in this
	// window.
	UpdateWithStartBetweenLockAndStart = NewKey("history.updateWithStart.betweenLockAndStart")
	// UpdateWithStartClosingWorkflowRetry fires before Update-with-Start retries after its update
	// was aborted by a closing workflow. Tests use Call to start a new workflow before the retry.
	UpdateWithStartClosingWorkflowRetry = NewKey("history.updateWithStart.closingWorkflowRetry")
)

var (
	keyNamesMu sync.Mutex
	keyNames   = map[string]struct{}{}
)

// NewKey returns the key for the named failpoint. Each name can only be declared once; NewKey
// panics on a duplicate so that two failpoints cannot share a name by accident.
func NewKey(name string) Key {
	keyNamesMu.Lock()
	defer keyNamesMu.Unlock()
	if _, ok := keyNames[name]; ok {
		panic(fmt.Sprintf("failpoint: duplicate key %q", name))
	}
	keyNames[name] = struct{}{}
	return Key{name: name}
}

// Name returns the failpoint name.
func (k Key) Name() string { return k.name }

// NamespaceID returns the namespace the key is scoped to, or "" for all namespaces.
func (k Key) NamespaceID() namespace.ID { return k.namespaceID }

// ForNamespace returns the key scoped to a namespace.
func (k Key) ForNamespace(nsID namespace.ID) Key {
	return Key{name: k.name, namespaceID: nsID}
}

func (k Key) String() string {
	if k.namespaceID == "" {
		return k.name
	}
	return fmt.Sprintf("%s@%s", k.name, k.namespaceID)
}
