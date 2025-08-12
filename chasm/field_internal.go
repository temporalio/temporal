package chasm

type fieldInternal struct {
	// These 2 fields are used when node is not set yet (i.e., node==nil).
	// Don't access them directly outside of this file. Use corresponding getters instead.
	ft fieldType
	v  any // Component | Data | Pointer

	// Pointer to the corresponding tree node. Can be nil for the just created fields.
	node *Node
}

func newFieldInternalWithValue(ft fieldType, v any) fieldInternal {
	return fieldInternal{
		ft: ft,
		v:  v,
	}
}

func newFieldInternalWithNode(node *Node) fieldInternal {
	return fieldInternal{
		node: node,
	}
}

func (fi fieldInternal) isEmpty() bool {
	return fi.v == nil && fi.node == nil
}

func (fi fieldInternal) value() any {
	// Deferred pointers are special-cased, since their serialized nodes are
	// initialized as regular persistable pointers.
	//
	// Deferred pointers may have a non-nil node after syncSubComponents, but before
	// resolution.
	if fi.node == nil || fi.ft == fieldTypeDeferredPointer {
		return fi.v
	}
	return fi.node.value
}

func (fi fieldInternal) fieldType() fieldType {
	// Deferred pointers are special-cased, since their serialized nodes are
	// initialized as regular persistable pointers.
	//
	// Deferred pointers may have a non-nil node after syncSubComponents, but before
	// resolution.
	if fi.node == nil || fi.ft == fieldTypeDeferredPointer {
		return fi.ft
	}
	return fi.node.fieldType()
}
