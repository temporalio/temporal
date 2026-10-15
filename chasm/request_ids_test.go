package chasm

import (
	"context"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/proto"
)

func (s *nodeSuite) TestRequestIDs_AttachAndHas() {
	node := s.testComponentTree()

	has, err := node.HasRequestID("req-1")
	s.NoError(err)
	s.False(has)
	s.NotContains(node.children, requestIDsNodeName, "a read must not create the request IDs node")

	s.NoError(node.AttachRequestID("req-1", 0, 0))
	has, err = node.HasRequestID("req-1")
	s.NoError(err)
	s.True(has)

	mutations, err := node.CloseTransaction(context.Background(), true)
	s.NoError(err)
	s.Contains(mutations.UpdatedNodes, requestIDsNodeName)
	s.NotNil(mutations.UpdatedNodes[requestIDsNodeName].GetMetadata().GetDataAttributes())

	has, err = node.HasRequestID("req-1")
	s.NoError(err)
	s.True(has)
	has, err = node.HasRequestID("req-2")
	s.NoError(err)
	s.False(has)
}

func (s *nodeSuite) TestRequestIDs_HasDoesNotMutate() {
	node := s.testComponentTree()
	s.NoError(node.AttachRequestID("req-1", 0, 0))
	_, err := node.CloseTransaction(context.Background(), true)
	s.NoError(err)

	has, err := node.HasRequestID("req-1")
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

	has, err := node.HasRequestID("req-1")
	s.NoError(err)
	s.True(has)
}

func (s *nodeSuite) TestRequestIDs_MaxCount() {
	node := s.testComponentTree()
	for _, id := range []string{"req-1", "req-2", "req-3", "req-4"} {
		s.NoError(node.AttachRequestID(id, 3, 0))
	}
	s.Equal([]string{"req-2", "req-3", "req-4"}, s.recordedRequestIDs(node))

	// Re-attaching an existing ID is a no-op and does not reorder.
	s.NoError(node.AttachRequestID("req-2", 3, 0))
	s.Equal([]string{"req-2", "req-3", "req-4"}, s.recordedRequestIDs(node))
}

func (s *nodeSuite) TestRequestIDs_MaxAge() {
	node := s.testComponentTree()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	maxAge := time.Hour

	s.timeSource.Update(start)
	s.NoError(node.AttachRequestID("req-1", 0, maxAge))
	s.timeSource.Update(start.Add(30 * time.Minute))
	s.NoError(node.AttachRequestID("req-2", 0, maxAge))

	// Expired entries are only dropped on the next attach, so a read still reports them.
	s.timeSource.Update(start.Add(90 * time.Minute))
	has, err := node.HasRequestID("req-1")
	s.NoError(err)
	s.True(has)

	s.NoError(node.AttachRequestID("req-3", 0, maxAge))
	s.Equal([]string{"req-2", "req-3"}, s.recordedRequestIDs(node))
}

func (s *nodeSuite) TestRequestIDs_RecordTimeTruncatedToSeconds() {
	node := s.testComponentTree()
	s.timeSource.Update(time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC))
	s.NoError(node.AttachRequestID("req-1", 0, 0))

	requestIDs, err := node.requestIDs(false)
	s.NoError(err)
	s.Len(requestIDs.GetEntries(), 1)
	s.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), requestIDs.GetEntries()[0].GetRecordTime().AsTime())
	s.Zero(requestIDs.GetEntries()[0].GetRecordTime().GetNanos())
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

	has, err := loaded.HasRequestID("req-1")
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
	s.Len(persisted.GetEntries(), 2)
	s.Equal("req-1", persisted.GetEntries()[0].GetRequestId())
	s.Equal("req-2", persisted.GetEntries()[1].GetRequestId())
}

func (s *nodeSuite) TestRequestIDs_NonRootNode() {
	node := s.testComponentTree()

	_, err := node.children["SubComponent1"].HasRequestID("req-1")
	s.Error(err)
	s.Error(node.children["SubComponent1"].AttachRequestID("req-1", 0, 0))
}

func (s *nodeSuite) recordedRequestIDs(node *Node) []string {
	requestIDs, err := node.requestIDs(false)
	s.NoError(err)
	ids := make([]string, 0, len(requestIDs.GetEntries()))
	for _, entry := range requestIDs.GetEntries() {
		ids = append(ids, entry.GetRequestId())
	}
	return ids
}

func (s *nodeSuite) TestRequestIDs_Replication_ApplyMutation() {
	active, standby := s.activeAndStandbyTrees()

	s.NoError(active.AttachRequestID("req-1", 0, 0))
	mutation, err := active.CloseTransaction(context.Background(), true)
	s.NoError(err)
	s.Contains(mutation.UpdatedNodes, requestIDsNodeName)

	s.NoError(standby.ApplyMutation(mutation))
	has, err := standby.HasRequestID("req-1")
	s.NoError(err)
	s.True(has, "a request ID recorded on the active cluster must be deduplicated on the standby")
}

func (s *nodeSuite) TestRequestIDs_Replication_ApplySnapshot() {
	active, standby := s.activeAndStandbyTrees()

	s.NoError(active.AttachRequestID("req-1", 0, 0))
	_, err := active.CloseTransaction(context.Background(), true)
	s.NoError(err)

	s.NoError(standby.ApplySnapshot(active.Snapshot(nil)))
	has, err := standby.HasRequestID("req-1")
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
