package chasm

import (
	"cmp"
	"maps"
	"reflect"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
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

// HasRequestID returns true if requestID was recorded via AttachRequestID, has not been dropped, and is
// not older than maxAge (maxAge <= 0 disables the age check). An expired entry is reported as absent; it
// stays in the node until AttachRequestID replaces it or drops it as one of the oldest.
// It must be called on the root node. It does not mark the node as mutated.
func (n *Node) HasRequestID(requestID string, maxAge time.Duration) (bool, error) {
	requestIDs, err := n.requestIDs(false)
	if err != nil || requestIDs == nil {
		return false, err
	}
	recordTime, ok := requestIDs.GetRequestIds()[requestID]
	return ok && !isRequestIDExpired(recordTime, n.backend.Now(), maxAge), nil
}

// AttachRequestID records requestID so that a later request reusing it can be deduplicated.
//
// Expired entries are not swept here; HasRequestID ignores them, and they are the first dropped when the
// count is trimmed. Once recording requestID would make more than maxCount entries, the oldest existing
// ones are dropped first so that half of maxCount remain after it is recorded; the cost of ordering entries
// by age is paid once per maxCount/2 attaches instead of on every attach. A value <= 0 disables the
// respective limit.
// It must be called on the root node, and only the request IDs node is marked as mutated.
func (n *Node) AttachRequestID(requestID string, maxCount int, maxAge time.Duration) error {
	requestIDs, err := n.requestIDs(true)
	if err != nil {
		return err
	}
	if requestIDs.RequestIds == nil {
		requestIDs.RequestIds = make(map[string]*timestamppb.Timestamp)
	}
	now := n.backend.Now()

	if recordTime, ok := requestIDs.RequestIds[requestID]; ok {
		// Callers check HasRequestID first under the same execution lock, so a live entry means a bug.
		// Fail instead of silently persisting a duplicate update.
		if !isRequestIDExpired(recordTime, now, maxAge) {
			return softassert.UnexpectedInternalErr(
				n.logger,
				"request ID is already recorded",
				nil,
				tag.String("request-id", requestID),
			)
		}
		// Expired: replace it.
		delete(requestIDs.RequestIds, requestID)
	}

	// Trim before recording requestID, so the new entry is never a candidate for dropping (record times
	// are truncated to seconds and may tie, so "newest" can't be told apart reliably after inserting).
	if maxCount > 0 && len(requestIDs.RequestIds) >= maxCount {
		dropOldestRequestIDs(requestIDs.RequestIds, max(maxCount/2, 1)-1)
	}
	requestIDs.RequestIds[requestID] = timestamppb.New(now.Truncate(time.Second))

	n.children[requestIDsNodeName].setValueState(valueStateNeedSerialize)
	return nil
}

// isRequestIDExpired reports whether a request ID recorded at recordTime is older than maxAge at now.
// maxAge <= 0 means request IDs never expire.
func isRequestIDExpired(recordTime *timestamppb.Timestamp, now time.Time, maxAge time.Duration) bool {
	return maxAge > 0 && recordTime.AsTime().Before(now.Add(-maxAge))
}

// dropOldestRequestIDs drops the oldest entries of requestIDs until keep entries remain. Entries
// recorded in the same second are ordered by request ID.
func dropOldestRequestIDs(requestIDs map[string]*timestamppb.Timestamp, keep int) {
	ids := slices.SortedFunc(maps.Keys(requestIDs), func(a, b string) int {
		return cmp.Or(requestIDs[a].AsTime().Compare(requestIDs[b].AsTime()), cmp.Compare(a, b))
	})
	for _, id := range ids[:len(ids)-keep] {
		delete(requestIDs, id)
	}
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
