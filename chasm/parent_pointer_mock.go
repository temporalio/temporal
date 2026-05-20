package chasm

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
)

// NewMockParentPtr creates a ParentPtr[T] backed by real Node instances
// that returns the given parent value when Get or TryGet is called.
// This is intended for use in unit tests where a full CHASM tree is not needed.
func NewMockParentPtr[T any](parent T) ParentPtr[T] {
	base := &nodeBase{
		logger: log.NewNoopLogger(),
	}

	parentNode := newNode(base, nil, "")
	parentNode.serializedNode = &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
				ComponentAttributes: &persistencespb.ChasmComponentAttributes{},
			},
		},
	}
	parentNode.value = parent
	parentNode.valueState = valueStateSynced

	childNode := newNode(base, parentNode, "mock_child")

	return ParentPtr[T]{
		Internal: parentPtrInternal{
			currentNode: childNode,
		},
	}
}
