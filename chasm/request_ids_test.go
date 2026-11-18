package chasm

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

func (s *nodeSuite) TestRequestIDs_AttachAndHas() {
	node := s.testComponentTree()

	has, err := node.HasRequestID("req-1", 0)
	s.NoError(err)
	s.False(has)
	s.NotContains(node.children, requestIDsNodeName, "a read must not create the request IDs node")

	s.NoError(node.AttachRequestID("req-1", 0, 0))
	has, err = node.HasRequestID("req-1", 0)
	s.NoError(err)
	s.True(has)

	mutations, err := node.CloseTransaction(context.Background(), true)
	s.NoError(err)
	s.Contains(mutations.UpdatedNodes, requestIDsNodeName)
	s.NotNil(mutations.UpdatedNodes[requestIDsNodeName].GetMetadata().GetDataAttributes())

	has, err = node.HasRequestID("req-1", 0)
	s.NoError(err)
	s.True(has)
	has, err = node.HasRequestID("req-2", 0)
	s.NoError(err)
	s.False(has)
}

func (s *nodeSuite) TestRequestIDs_HasDoesNotMutate() {
	node := s.testComponentTree()
	s.NoError(node.AttachRequestID("req-1", 0, 0))
	_, err := node.CloseTransaction(context.Background(), true)
	s.NoError(err)

	has, err := node.HasRequestID("req-1", 0)
	s.NoError(err)
	s.True(has)

	mutations, err := node.CloseTransaction(context.Background(), true)
	s.NoError(err)
	s.Empty(mutations.UpdatedNodes)
	s.Empty(mutations.DeletedNodes)
}

func (s *nodeSuite) TestRequestIDs_SurvivesRootStructureSync() {
	node := s.testComponentTree()
	s.NoError(node.AttachRequestID("req-1", 0, 0))
	_, err := node.CloseTransaction(context.Background(), true)
	s.NoError(err)

	// Mutable access to the root triggers a structure sync, which deletes children not backed by a field.
	chasmCtx := NewMutableContext(context.Background(), node)
	tc, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	tc.(*TestComponent).ComponentData = &protoMessageType{CreateRequestId: "updated"}

	mutations, err := node.CloseTransaction(context.Background(), true)
	s.NoError(err)
	s.Contains(mutations.UpdatedNodes, "")
	s.NotContains(mutations.UpdatedNodes, requestIDsNodeName, "request IDs node must not be rewritten when unchanged")
	s.NotContains(mutations.DeletedNodes, requestIDsNodeName)

	has, err := node.HasRequestID("req-1", 0)
	s.NoError(err)
	s.True(has)
}

func (s *nodeSuite) TestRequestIDs_MaxCount() {
	node := s.testComponentTree()
	const maxCount = 4
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	attach := func(i int) {
		s.timeSource.Update(start.Add(time.Duration(i) * time.Second))
		s.NoError(node.AttachRequestID(fmt.Sprintf("req-%d", i), maxCount, 0))
	}

	// Up to maxCount entries are kept as is.
	for i := 1; i <= maxCount; i++ {
		attach(i)
	}
	s.Equal([]string{"req-1", "req-2", "req-3", "req-4"}, s.recordedRequestIDs(node))

	// Going over maxCount drops the oldest until maxCount/2 remain.
	attach(5)
	s.Equal([]string{"req-4", "req-5"}, s.recordedRequestIDs(node))

	// Then entries accumulate again until the next time maxCount is exceeded.
	attach(6)
	s.Equal([]string{"req-4", "req-5", "req-6"}, s.recordedRequestIDs(node))

	// Re-attaching an existing ID is a bug in the caller (it must check HasRequestID first): it fails with
	// an internal error and leaves the recorded IDs unchanged.
	tl, ok := s.logger.(*testlogger.TestLogger)
	s.Require().True(ok)
	tl.Expect(testlogger.Error, "request ID is already recorded")
	err := node.AttachRequestID("req-5", maxCount, 0)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.Equal([]string{"req-4", "req-5", "req-6"}, s.recordedRequestIDs(node))
}

func (s *nodeSuite) TestRequestIDs_MaxCountKeepsNewID() {
	node := s.testComponentTree()
	// All in the same second, and the new ID sorts first. Adding it goes over the limit of 2, so the
	// entries are trimmed down to 1; the existing ones are trimmed before it is recorded, so it is kept.
	for _, id := range []string{"req-x", "req-y"} {
		s.NoError(node.AttachRequestID(id, 2, 0))
	}
	s.NoError(node.AttachRequestID("req-a", 2, 0))

	has, err := node.HasRequestID("req-a", 0)
	s.NoError(err)
	s.True(has, "the just-recorded request ID must never be dropped")
	s.Len(s.recordedRequestIDs(node), 1)
}

func (s *nodeSuite) TestRequestIDs_MaxAge() {
	node := s.testComponentTree()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	maxAge := time.Hour

	s.timeSource.Update(start)
	s.NoError(node.AttachRequestID("req-1", 0, maxAge))
	s.timeSource.Update(start.Add(30 * time.Minute))
	s.NoError(node.AttachRequestID("req-2", 0, maxAge))

	// Past maxAge, req-1 is no longer reported, while req-2 still is.
	s.timeSource.Update(start.Add(90 * time.Minute))
	has, err := node.HasRequestID("req-1", maxAge)
	s.NoError(err)
	s.False(has, "a request ID older than maxAge is not a duplicate")
	has, err = node.HasRequestID("req-2", maxAge)
	s.NoError(err)
	s.True(has)

	// With the age check disabled, the expired entry is still there: it is not swept, only ignored.
	has, err = node.HasRequestID("req-1", 0)
	s.NoError(err)
	s.True(has)

	// Recording another ID doesn't sweep it either; it is dropped only when the count limit is exceeded.
	s.NoError(node.AttachRequestID("req-3", 0, maxAge))
	s.Equal([]string{"req-1", "req-2", "req-3"}, s.recordedRequestIDs(node))
}

func (s *nodeSuite) TestRequestIDs_ExpiredEntryIsReplaced() {
	node := s.testComponentTree()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	maxAge := time.Hour

	s.timeSource.Update(start)
	s.NoError(node.AttachRequestID("req-1", 0, maxAge))

	// Past maxAge, recording the same ID again replaces the expired entry instead of failing as a duplicate.
	later := start.Add(2 * time.Hour)
	s.timeSource.Update(later)
	s.NoError(node.AttachRequestID("req-1", 0, maxAge))

	requestIDs, err := node.requestIDs(false)
	s.NoError(err)
	s.Len(requestIDs.GetRequestIds(), 1)
	s.Equal(later, requestIDs.GetRequestIds()["req-1"].AsTime())
}

func (s *nodeSuite) TestRequestIDs_RecordTimeTruncatedToSeconds() {
	node := s.testComponentTree()
	s.timeSource.Update(time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC))
	s.NoError(node.AttachRequestID("req-1", 0, 0))

	requestIDs, err := node.requestIDs(false)
	s.NoError(err)
	s.Len(requestIDs.GetRequestIds(), 1)
	recordTime := requestIDs.GetRequestIds()["req-1"]
	s.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), recordTime.AsTime())
	s.Zero(recordTime.GetNanos())
}

func (s *nodeSuite) TestRequestIDs_LoadFromDB() {
	node := s.testComponentTree()
	s.NoError(node.AttachRequestID("req-1", 0, 0))
	_, err := node.CloseTransaction(context.Background(), true)
	s.NoError(err)

	snapshot := node.Snapshot(nil)
	s.Contains(snapshot.Nodes, requestIDsNodeName)

	loaded, err := s.newTestTree(snapshot.Nodes)
	s.NoError(err)

	has, err := loaded.HasRequestID("req-1", 0)
	s.NoError(err)
	s.True(has)

	// Structure sync on the reloaded tree keeps the node too.
	chasmCtx := NewMutableContext(context.Background(), loaded)
	_, err = loaded.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	mutations, err := loaded.CloseTransaction(context.Background(), true)
	s.NoError(err)
	s.NotContains(mutations.DeletedNodes, requestIDsNodeName)

	s.NoError(loaded.AttachRequestID("req-2", 0, 0))
	mutations, err = loaded.CloseTransaction(context.Background(), true)
	s.NoError(err)
	s.Contains(mutations.UpdatedNodes, requestIDsNodeName)

	value, err := unmarshalProto(mutations.UpdatedNodes[requestIDsNodeName].GetData(), requestIDsValueT)
	s.NoError(err)
	persisted := value.Interface().(*persistencespb.ChasmRequestIDs)
	s.ElementsMatch([]string{"req-1", "req-2"}, slices.Collect(maps.Keys(persisted.GetRequestIds())))
}

func (s *nodeSuite) TestRequestIDs_NonRootNode() {
	node := s.testComponentTree()

	_, err := node.children["SubComponent1"].HasRequestID("req-1", 0)
	s.Error(err)
	s.Error(node.children["SubComponent1"].AttachRequestID("req-1", 0, 0))
}

// recordedRequestIDs returns the recorded request IDs, sorted.
func (s *nodeSuite) recordedRequestIDs(node *Node) []string {
	requestIDs, err := node.requestIDs(false)
	s.NoError(err)
	return slices.Sorted(maps.Keys(requestIDs.GetRequestIds()))
}

func (s *nodeSuite) TestRequestIDs_Replication_ApplyMutation() {
	active, standby := s.activeAndStandbyTrees()

	s.NoError(active.AttachRequestID("req-1", 0, 0))
	mutation, err := active.CloseTransaction(context.Background(), true)
	s.NoError(err)
	s.Contains(mutation.UpdatedNodes, requestIDsNodeName)

	s.NoError(standby.ApplyMutation(mutation))
	has, err := standby.HasRequestID("req-1", 0)
	s.NoError(err)
	s.True(has, "a request ID recorded on the active cluster must be deduplicated on the standby")
}

func (s *nodeSuite) TestRequestIDs_Replication_ApplySnapshot() {
	active, standby := s.activeAndStandbyTrees()

	s.NoError(active.AttachRequestID("req-1", 0, 0))
	_, err := active.CloseTransaction(context.Background(), true)
	s.NoError(err)

	s.NoError(standby.ApplySnapshot(active.Snapshot(nil)))
	has, err := standby.HasRequestID("req-1", 0)
	s.NoError(err)
	s.True(has, "a request ID recorded on the active cluster must be deduplicated on the standby")
}

// activeAndStandbyTrees returns two trees with the same persisted state and no request IDs recorded.
func (s *nodeSuite) activeAndStandbyTrees() (active *Node, standby *Node) {
	active = s.testComponentTree()
	_, err := active.CloseTransaction(context.Background(), true)
	s.NoError(err)

	nodes := make(map[string]*persistencespb.ChasmNode)
	for path, node := range active.Snapshot(nil).Nodes {
		nodes[path] = proto.Clone(node).(*persistencespb.ChasmNode)
	}
	standby, err = s.newTestTree(nodes)
	s.NoError(err)
	return active, standby
}
