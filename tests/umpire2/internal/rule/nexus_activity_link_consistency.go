package rule

import (
	"slices"
	"strings"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/internal/model"
	"go.temporal.io/server/tests/umpire2/internal/protocol"
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
	verificationModel, state := relationVerificationState(c.Config.Relations, operations, activities, changedNamespaces)
	properties := protocol.NexusActivityLinkConsistencyProperties()
	for _, edge := range c.Config.Relations.Snapshot() {
		if !changedNamespaces[relationNamespace(edge.Source.ID)] {
			continue
		}
		var property verify.Property
		var message, operationID, activityID string
		switch edge.Type {
		case protocol.NexusActivityRelation:
			if operations[edge.Source.ID] == nil || activities[edge.Target.ID] == nil {
				continue
			}
			property = properties[0]
			message = "Nexus operation Activity link has no matching activity back-link"
			operationID, activityID = edge.Source.ID, edge.Target.ID
		case protocol.ActivityNexusRelation:
			if activities[edge.Source.ID] == nil || operations[edge.Target.ID] == nil {
				continue
			}
			property = properties[1]
			message = "activity Nexus operation link has no matching Nexus-side link"
			operationID, activityID = edge.Target.ID, edge.Source.ID
		default:
			continue
		}
		if len(property.Expr.Args) != 1 || len(property.Expr.Args[0].Args) != 1 {
			c.Eval(edge.Source.ID+":"+edge.Target.ID, false, umpire.Violation{Message: "invalid generated Nexus activity consistency property shape"})
			continue
		}
		condition := property.Expr.Args[0].Args[0]
		holds, err := verify.EvaluateExpr(verificationModel, state, condition, verify.Bindings{
			"source": edge.Source.ID,
			"target": edge.Target.ID,
		})
		if err != nil {
			c.Eval(edge.Source.ID+":"+edge.Target.ID, false, umpire.Violation{Message: "invalid generated Nexus activity consistency property: " + err.Error()})
			continue
		}
		c.Eval(edge.Source.ID+":"+edge.Target.ID, holds, umpire.Violation{
			Message: message,
			Tags: map[string]string{
				"operationID": relationSubject(operationID),
				"activityID":  relationSubject(activityID),
			},
		})
	}
}

func relationVerificationState(
	relations *umpire.RelationStore,
	operations map[string]*model.NexusOperation,
	activities map[string]*model.Activity,
	changedNamespaces map[string]bool,
) (verify.Model, verify.ModelState) {
	operationIDs := verificationEntityIDs(operations, changedNamespaces)
	activityIDs := verificationEntityIDs(activities, changedNamespaces)
	verificationModel := verify.Model{
		Version: "umpire2/runtime-link-consistency/v1",
		Entities: []verify.EntityType{
			{Name: string(model.NexusOperationType), IDs: operationIDs},
			{Name: string(model.ActivityType), IDs: activityIDs},
		},
		Relations: []verify.Relation{
			{Name: string(protocol.NexusActivityRelation), Source: string(model.NexusOperationType), Target: string(model.ActivityType), SourceCardinality: verify.One, TargetCardinality: verify.One},
			{Name: string(protocol.ActivityNexusRelation), Source: string(model.ActivityType), Target: string(model.NexusOperationType), SourceCardinality: verify.One, TargetCardinality: verify.One},
		},
	}
	state := verify.ModelState{
		Entities: map[string]map[string]string{
			string(model.NexusOperationType): existingVerificationEntities(operationIDs),
			string(model.ActivityType):       existingVerificationEntities(activityIDs),
		},
		Relations: map[string][]verify.RelationTuple{
			string(protocol.NexusActivityRelation): nil,
			string(protocol.ActivityNexusRelation): nil,
		},
	}
	for _, edge := range relations.Snapshot() {
		if !changedNamespaces[relationNamespace(edge.Source.ID)] {
			continue
		}
		if edge.Type != protocol.NexusActivityRelation && edge.Type != protocol.ActivityNexusRelation {
			continue
		}
		state.Relations[string(edge.Type)] = append(state.Relations[string(edge.Type)], verify.RelationTuple{Source: edge.Source.ID, Target: edge.Target.ID})
	}
	return verificationModel, state
}

func verificationEntityIDs[T any](entities map[string]T, changedNamespaces map[string]bool) []string {
	result := make([]string, 0, len(entities))
	for id := range entities {
		if changedNamespaces[relationNamespace(id)] {
			result = append(result, id)
		}
	}
	slices.Sort(result)
	return result
}

func existingVerificationEntities(ids []string) map[string]string {
	result := make(map[string]string, len(ids))
	for _, id := range ids {
		result[id] = ""
	}
	return result
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
