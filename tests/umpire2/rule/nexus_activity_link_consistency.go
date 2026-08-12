package rule

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
)

// NexusActivityLinkConsistency checks that public Nexus and activity links form matching pairs.
type NexusActivityLinkConsistency struct{}

func (*NexusActivityLinkConsistency) Name() string { return "NexusActivityLinkConsistencyRule" }

func (*NexusActivityLinkConsistency) CheckSafety(c *umpire.SafetyContext) {
	operations := map[string]*model.NexusOperation{}
	activities := map[string]*model.Activity{}
	changed := false
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
		switch result.Entity.(type) {
		case *model.NexusOperation, *model.Activity:
			changed = true
		default:
			continue
		}
	}
	if !changed {
		return
	}
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

func linkSubjectKey(namespaceID, id string) string {
	return namespaceID + "\x00" + id
}
