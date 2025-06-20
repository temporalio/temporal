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
	Extensions interface{}
}

// @@@SNIPEND
