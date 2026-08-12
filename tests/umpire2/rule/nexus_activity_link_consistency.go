package rule

import (
	"slices"
	"strings"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/protocol"
)

// NexusActivityLinkConsistency checks that public Nexus and activity links form matching pairs.
type NexusActivityLinkConsistency struct{}

func (*NexusActivityLinkConsistency) Name() string { return "NexusActivityLinkConsistencyRule" }

func (*NexusActivityLinkConsistency) CheckSafety(c *umpire.SafetyContext) {
	operations := map[string]*model.NexusOperation{}
	activities := map[string]*model.Activity{}
	changedNamespaces := map[string]bool{}
	for _, entry := range c.ModelState.QueryEntities(model.NexusOperationType, 0, nil) {
		if operation, ok := entry.Entity.(*model.NexusOperation); ok {
			operations[linkSubjectKey(operation.NamespaceID, operation.WorkflowID)] = operation
		}
	}
	for _, entry := range c.ModelState.QueryEntities(model.ActivityType, 0, nil) {
		if activity, ok := entry.Entity.(*model.Activity); ok {
			activities[linkSubjectKey(activity.NamespaceID, activity.ActivityID)] = activity
		}
	}
	for result := range c.ChangedLifecycles() {
		switch entity := result.Entity.(type) {
		case *model.NexusOperation:
			changedNamespaces[entity.NamespaceID] = true
		case *model.Activity:
			changedNamespaces[entity.NamespaceID] = true
		default:
			continue
		}
	}
	if len(changedNamespaces) == 0 {
		return
	}
	if c.Config.Relations != nil {
		checkRelationLinks(c, operations, activities, changedNamespaces)
		return
	}
	checkLegacyLinks(c, operations, activities)
}

func checkRelationLinks(
	c *umpire.SafetyContext,
	operations map[string]*model.NexusOperation,
	activities map[string]*model.Activity,
	changedNamespaces map[string]bool,
) {
	for _, edge := range c.Config.Relations.Snapshot() {
		namespaceID := relationNamespace(edge.Source.ID)
		if !changedNamespaces[namespaceID] {
			continue
		}
		switch edge.Type {
		case protocol.NexusActivityRelation:
			if operations[edge.Source.ID] == nil || activities[edge.Target.ID] == nil {
				continue
			}
			matched := slices.Contains(c.Config.Relations.Targets(protocol.ActivityNexusRelation, edge.Target), edge.Source)
			c.Eval(edge.Source.ID+":"+edge.Target.ID, matched, umpire.Violation{
				Message: "Nexus operation Activity link has no matching activity back-link",
				Tags: map[string]string{
					"operationID": relationSubject(edge.Source.ID),
					"activityID":  relationSubject(edge.Target.ID),
				},
			})
		case protocol.ActivityNexusRelation:
			if activities[edge.Source.ID] == nil || operations[edge.Target.ID] == nil {
				continue
			}
			matched := slices.Contains(c.Config.Relations.Targets(protocol.NexusActivityRelation, edge.Target), edge.Source)
			c.Eval(edge.Source.ID+":"+edge.Target.ID, matched, umpire.Violation{
				Message: "activity Nexus operation link has no matching Nexus-side link",
				Tags: map[string]string{
					"operationID": relationSubject(edge.Target.ID),
					"activityID":  relationSubject(edge.Source.ID),
				},
			})
		default:
			continue
		}
	}
}

func checkLegacyLinks(c *umpire.SafetyContext, operations map[string]*model.NexusOperation, activities map[string]*model.Activity) {
	for _, operation := range operations {
		operationID := operation.WorkflowID
		for _, link := range operation.Links {
			activityLink := link.GetActivity()
			if activityLink == nil {
				continue
			}
			activity := activities[linkSubjectKey(operation.NamespaceID, activityLink.GetActivityId())]
			if activity == nil {
				continue
			}
			matched := false
			for _, reverse := range activity.Links {
				if reverse.GetNexusOperation().GetOperationId() == operationID {
					matched = true
					break
				}
			}
			c.Eval(operationID+":"+activityLink.GetActivityId(), matched, umpire.Violation{
				Message: "Nexus operation Activity link has no matching activity back-link",
				Tags:    map[string]string{"operationID": operationID, "activityID": activityLink.GetActivityId()},
			})
		}
	}
	for _, activity := range activities {
		activityID := activity.ActivityID
		for _, link := range activity.Links {
			operationLink := link.GetNexusOperation()
			if operationLink == nil {
				continue
			}
			operation := operations[linkSubjectKey(activity.NamespaceID, operationLink.GetOperationId())]
			if operation == nil {
				continue
			}
			matched := false
			for _, reverse := range operation.Links {
				if reverse.GetActivity().GetActivityId() == activityID {
					matched = true
					break
				}
			}
			c.Eval(activityID+":"+operationLink.GetOperationId(), matched, umpire.Violation{
				Message: "activity Nexus operation link has no matching Nexus-side link",
				Tags:    map[string]string{"operationID": operationLink.GetOperationId(), "activityID": activityID},
			})
		}
	}
}

func relationNamespace(id string) string {
	namespaceID, _, _ := strings.Cut(id, "\x00")
	return namespaceID
}

func relationSubject(id string) string {
	_, subject, _ := strings.Cut(id, "\x00")
	return subject
}

func linkSubjectKey(namespaceID, id string) string {
	return namespaceID + "\x00" + id
}
