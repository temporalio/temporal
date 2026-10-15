package chasm

import (
	"reflect"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// requestIDsNodeName is the name of the framework-owned data node that records request IDs accepted by
// UpdateComponent, so that a retried request can be deduplicated.
//
// The node is a reserved child of the root node: it is not declared as a field by any component, so
// syncSubComponents must explicitly keep it (see reservedRootChildren). Keeping it in its own node, rather
// than in the execution state, means it is only written when a request ID is added.
const requestIDsNodeName = reservedNodeNamePrefix + "_requestIDs"

// reservedNodeNamePrefix is reserved for framework-owned node names. Component fields must not use a name
// with this prefix (see Registry.validateFieldNames), so they can't collide with framework-owned nodes.
const reservedNodeNamePrefix = "__chasm"

var requestIDsValueT = reflect.TypeFor[*persistencespb.ChasmRequestIDs]()

// reservedRootChildren are framework-owned children of the root node that are not backed by a component
// field and must survive the root's structure sync.
var reservedRootChildren = []string{requestIDsNodeName}

// HasRequestID returns true if requestID was previously recorded via AttachRequestID and has not been
// dropped yet. An entry past its max age is still reported until the next AttachRequestID drops it.
// It must be called on the root node. It does not mark the node as mutated.
func (n *Node) HasRequestID(requestID string) (bool, error) {
	requestIDs, err := n.requestIDs(false)
	if err != nil || requestIDs == nil {
		return false, err
	}
	return containsRequestID(requestIDs.GetEntries(), requestID), nil
}

// AttachRequestID records requestID so that a later request reusing it can be deduplicated.
//
// Entries are kept in the order they were recorded, and the oldest are dropped from the front when
// older than maxAge or beyond maxCount; a value <= 0 disables the respective limit.
// It must be called on the root node, and only the request IDs node is marked as mutated.
func (n *Node) AttachRequestID(requestID string, maxCount int, maxAge time.Duration) error {
	requestIDs, err := n.requestIDs(true)
	if err != nil {
		return err
	}
	if containsRequestID(requestIDs.GetEntries(), requestID) {
		return nil
	}

	now := n.backend.Now().Truncate(time.Second)
	entries := append(requestIDs.Entries, &persistencespb.ChasmRequestIDs_Entry{
		RequestId:  requestID,
		RecordTime: timestamppb.New(now),
	})

	// Entries are appended in time order, so expired ones are at the front. The new entry is never expired.
	if maxAge > 0 {
		cutoff := now.Add(-maxAge)
		expired := slices.IndexFunc(entries, func(e *persistencespb.ChasmRequestIDs_Entry) bool {
			return !e.GetRecordTime().AsTime().Before(cutoff)
		})
		entries = slices.Delete(entries, 0, expired)
	}
	if maxCount > 0 && len(entries) > maxCount {
		entries = slices.Delete(entries, 0, len(entries)-maxCount)
	}

	requestIDs.Entries = entries
	n.children[requestIDsNodeName].setValueState(valueStateNeedSerialize)
	return nil
}

func containsRequestID(entries []*persistencespb.ChasmRequestIDs_Entry, requestID string) bool {
	return slices.ContainsFunc(entries, func(e *persistencespb.ChasmRequestIDs_Entry) bool {
		return e.GetRequestId() == requestID
	})
}

// requestIDs returns the deserialized value of the request IDs node. If the node doesn't exist, it is
// created when create is true, otherwise nil is returned.
func (n *Node) requestIDs(create bool) (*persistencespb.ChasmRequestIDs, error) {
	if n.parent != nil {
		return nil, serviceerror.NewInternal("request IDs can only be accessed on the root node")
	}

	childNode, ok := n.children[requestIDsNodeName]
	if !ok {
		if !create {
			return nil, nil
		}
		childNode = newNode(n.nodeBase, n, requestIDsNodeName)
		childNode.initSerializedNode(fieldTypeData)
		childNode.setValue(&persistencespb.ChasmRequestIDs{})
		childNode.setValueState(valueStateNeedSerialize)
		n.children[requestIDsNodeName] = childNode
	}

	if !childNode.isData() {
		return nil, serviceerror.NewInternalf("node %s is not a data node", requestIDsNodeName)
	}
	if err := childNode.deserialize(requestIDsValueT); err != nil {
		return nil, err
	}

	//nolint:revive // value is guaranteed to be of type *ChasmRequestIDs after deserialize.
	return childNode.value.(*persistencespb.ChasmRequestIDs), nil
}
