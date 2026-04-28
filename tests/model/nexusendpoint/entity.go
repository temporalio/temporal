// Package nexusendpoint models Temporal nexus endpoint registration as a
// property-test component.
package nexusendpoint

type Status int

const (
	StatusCreated Status = iota
	StatusDeleted // terminal
)

// Entity is the entity record stored in World.Endpoints.
type Entity struct {
	Name      string
	ID        string
	Version   int64
	Namespace string
	TaskQueue string
	Status    Status
}

// IsTerminal reports whether s is an absorbing status.
func IsTerminal(s Status) bool { return s == StatusDeleted }
