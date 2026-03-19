package umpire

import (
	"testing"
)

// ── test helpers ──────────────────────────────────────────────────────────────

type testFact struct {
	factType string
	target   *Identity
}

func (m *testFact) Name() string            { return m.factType }
func (m *testFact) TargetEntity() *Identity { return m.target }

func newTestFact(factType string, entityType EntityType, id string) *testFact {
	eid := NewEntityID(entityType, id)
	return &testFact{factType: factType, target: &Identity{EntityID: eid}}
}

func newTestFactWithParent(factType string, entityType EntityType, id string, parentType EntityType, parentID string) *testFact {
	eid := NewEntityID(entityType, id)
	pid := NewEntityID(parentType, parentID)
	return &testFact{factType: factType, target: &Identity{EntityID: eid, ParentID: &pid}}
}

// ── Add / All ─────────────────────────────────────────────────────────────────

func TestFactLog_AddAndAll(t *testing.T) {
	sb := NewFactLog()
	if len(sb.All()) != 0 {
		t.Fatal("expected empty event log")
	}
	m1 := newTestFact("typeA", "Workflow", "wf1")
	m2 := newTestFact("typeB", "Workflow", "wf2")
	sb.Add(m1)
	sb.Add(m2)
	all := sb.All()
	if len(all) != 2 {
		t.Fatalf("expected 2 events, got %d", len(all))
	}
}

func TestFactLog_AddAll(t *testing.T) {
	sb := NewFactLog()
	events := []Fact{
		newTestFact("typeA", "Workflow", "wf1"),
		newTestFact("typeA", "Workflow", "wf2"),
		newTestFact("typeB", "Task", "t1"),
	}
	sb.AddAll(events)
	if len(sb.All()) != 3 {
		t.Fatalf("expected 3 events, got %d", len(sb.All()))
	}
}

// ── QueryByType ───────────────────────────────────────────────────────────────

func TestFactLog_QueryByType_ExactMatch(t *testing.T) {
	sb := NewFactLog()
	sb.Add(newTestFact("addTask", "Workflow", "wf1"))
	sb.Add(newTestFact("addTask", "Workflow", "wf2"))
	sb.Add(newTestFact("pollTask", "Workflow", "wf1"))

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByType(eid, "addTask")
	if len(results) != 1 {
		t.Fatalf("expected 1 match, got %d", len(results))
	}
	if results[0].Name() != "addTask" {
		t.Fatalf("unexpected event type: %s", results[0].Name())
	}
}

func TestFactLog_QueryByType_NoFalsePositives_WrongType(t *testing.T) {
	sb := NewFactLog()
	// Same ID but different entity type — must not match
	sb.Add(newTestFact("addTask", "TaskQueue", "wf1"))

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByType(eid, "addTask")
	if len(results) != 0 {
		t.Fatalf("expected 0 matches (wrong entity type), got %d", len(results))
	}
}

func TestFactLog_QueryByType_NoFalsePositives_WrongID(t *testing.T) {
	sb := NewFactLog()
	sb.Add(newTestFact("addTask", "Workflow", "wf2"))

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByType(eid, "addTask")
	if len(results) != 0 {
		t.Fatalf("expected 0 matches (wrong ID), got %d", len(results))
	}
}

func TestFactLog_QueryByType_SubstringNotMatched(t *testing.T) {
	sb := NewFactLog()
	// "wf" is a prefix of "wf1" — must not match
	sb.Add(newTestFact("addTask", "Workflow", "wf"))

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByType(eid, "addTask")
	if len(results) != 0 {
		t.Fatalf("expected 0 matches (substring should not match), got %d", len(results))
	}
}

// ── QueryByID ─────────────────────────────────────────────────────────────────

func TestFactLog_QueryByID_ExactMatch(t *testing.T) {
	sb := NewFactLog()
	sb.Add(newTestFact("addTask", "Workflow", "wf1"))
	sb.Add(newTestFact("pollTask", "Workflow", "wf1"))
	sb.Add(newTestFact("addTask", "Workflow", "wf2"))

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByID(eid)
	if len(results) != 2 {
		t.Fatalf("expected 2 matches for wf1, got %d", len(results))
	}
}

func TestFactLog_QueryByID_NoFalsePositives_WrongType(t *testing.T) {
	sb := NewFactLog()
	sb.Add(newTestFact("event", "TaskQueue", "wf1"))

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByID(eid)
	if len(results) != 0 {
		t.Fatalf("expected 0 matches (wrong entity type), got %d", len(results))
	}
}

// ── Parent ID matching ────────────────────────────────────────────────────────

func TestFactLog_QueryByID_MatchesChildByParent(t *testing.T) {
	sb := NewFactLog()
	// child entity (WorkflowTask) with parent (Workflow "wf1")
	sb.Add(newTestFactWithParent("addTask", "WorkflowTask", "tq:wf1:run1", "Workflow", "wf1"))

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByID(eid)
	if len(results) != 1 {
		t.Fatalf("expected 1 match via parent ID, got %d", len(results))
	}
}

func TestFactLog_QueryByType_MatchesChildByParent(t *testing.T) {
	sb := NewFactLog()
	sb.Add(newTestFactWithParent("addTask", "WorkflowTask", "tq:wf1:run1", "Workflow", "wf1"))

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByType(eid, "addTask")
	if len(results) != 1 {
		t.Fatalf("expected 1 match via parent ID, got %d", len(results))
	}
}

func TestFactLog_QueryByID_NoFalsePositives_WrongParent(t *testing.T) {
	sb := NewFactLog()
	sb.Add(newTestFactWithParent("addTask", "WorkflowTask", "tq:wf2:run1", "Workflow", "wf2"))

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByID(eid)
	if len(results) != 0 {
		t.Fatalf("expected 0 matches (wrong parent), got %d", len(results))
	}
}

// ── Clear ─────────────────────────────────────────────────────────────────────

func TestFactLog_Clear(t *testing.T) {
	sb := NewFactLog()
	sb.Add(newTestFact("event", "Workflow", "wf1"))
	sb.Add(newTestFact("event", "Workflow", "wf2"))
	sb.Clear()
	if len(sb.All()) != 0 {
		t.Fatalf("expected empty event log after Clear, got %d", len(sb.All()))
	}
}

func TestFactLog_NilTarget_Ignored(t *testing.T) {
	sb := NewFactLog()
	// Event with nil TargetEntity should not panic
	nilEvent := &testFact{factType: "event", target: nil}
	sb.Add(nilEvent)

	eid := NewEntityID("Workflow", "wf1")
	results := sb.QueryByID(eid)
	if len(results) != 0 {
		t.Fatalf("expected 0 matches for nil target, got %d", len(results))
	}
}
