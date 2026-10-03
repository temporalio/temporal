package authorization

type Role int16

// @@@SNIPSTART temporal-common-authorization-role-enum
// User authz within the context of an entity, such as system, namespace or workflow.
// User may have any combination of these authz within each context, except for RoleUndefined, as a bitmask.
const (
	RoleWorker = Role(1 << iota)
	RoleReader
	RoleWriter
	RoleAdmin
	RoleUndefined = Role(0)
)

// @@@SNIPEND

// Checks if the provided role bitmask represents a valid combination of authz
func (b Role) IsValid() bool {
	return b&^(RoleWorker|RoleReader|RoleWriter|RoleAdmin) == 0
}

// @@@SNIPSTART temporal-common-authorization-claims
// Claims contains the identity of the subject and subject's roles at the system level and for individual namespaces
type Claims struct {
	// Identity of the subject
	Subject string
	// Role within the context of the whole Temporal cluster or a multi-cluster setup
	System Role
	// Roles within specific namespaces
	Namespaces map[string]Role
	// Free form bucket for extra data
	Extensions any
	// AuthType identifies the authentication method that produced these claims (e.g., "jwt", "mtls").
	AuthType string
}

// @@@SNIPEND

// hasAnyNamespaceRole reports whether these claims hold at least the given role on any
// individual namespace. Used by the default authorizer to let a namespace-scoped reader
// reach cluster-level readonly APIs that return low-sensitivity metadata rather than
// requiring a separate System-level claim for them (see Authorize's doc comment).
func (c *Claims) hasAnyNamespaceRole(role Role) bool {
	for _, r := range c.Namespaces {
		if r >= role {
			return true
		}
	}
	return false
}
