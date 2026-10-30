package chasm

const (
	// reservedNodeNamePrefix is reserved for framework-owned node names. Component fields must not use a name
	// with this prefix (see validateFieldNames), so they can't collide with framework-owned nodes.
	reservedNodeNamePrefix = "__chasm"

	// requestIDsNodeName is the name of the framework-owned data node that records request IDs accepted by
	// UpdateComponent, so that a retried request can be deduplicated.
	//
	// The node is a reserved child of the root node: it is not declared as a field by any component, so
	// syncSubComponents must explicitly keep it (see reservedRootChildren). Keeping it in its own node, rather
	// than in the execution state, means it is only written when a request ID is added.
	requestIDsNodeName = reservedNodeNamePrefix + "_requestIDs"
)
