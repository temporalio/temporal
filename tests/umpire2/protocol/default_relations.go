package protocol

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
	"go.temporal.io/server/tests/umpire2/model"
)

const (
	WorkflowRunsRelation         umpire.RelationType = "workflow-runs"
	WorkflowRunSuccessorRelation umpire.RelationType = "workflow-run-successor"
	NexusActivityRelation        umpire.RelationType = "nexus-activity"
	ActivityNexusRelation        umpire.RelationType = "activity-nexus"
)

func defaultRelationSchemas() []umpire.RelationSchema {
	return []umpire.RelationSchema{
		{
			Type:              WorkflowRunsRelation,
			Source:            model.WorkflowType,
			Target:            model.WorkflowRunType,
			SourceCardinality: umpire.RelationMany,
			TargetCardinality: umpire.RelationOne,
		},
		{
			Type:              WorkflowRunSuccessorRelation,
			Source:            model.WorkflowRunType,
			Target:            model.WorkflowRunType,
			SourceCardinality: umpire.RelationOne,
			TargetCardinality: umpire.RelationOne,
		},
		{
			Type:              NexusActivityRelation,
			Source:            model.NexusOperationType,
			Target:            model.ActivityType,
			SourceCardinality: umpire.RelationOne,
			TargetCardinality: umpire.RelationOne,
		},
		{
			Type:              ActivityNexusRelation,
			Source:            model.ActivityType,
			Target:            model.NexusOperationType,
			SourceCardinality: umpire.RelationOne,
			TargetCardinality: umpire.RelationOne,
		},
	}
}

func defaultRelationDerivers() []RelationDeriver {
	return []RelationDeriver{deriveWorkflowRunRelations, deriveNexusActivityRelations}
}

func deriveWorkflowRunRelations(observed umpire.Fact) []RelationMutation {
	started, ok := observed.(*fact.WorkflowRunStarted)
	if !ok || started.NamespaceID == "" || started.WorkflowID == "" || started.RunID == "" {
		return nil
	}
	run := scopedRelationEntity(model.WorkflowRunType, started.NamespaceID, started.RunID)
	scope := umpire.NewEntityID(model.NamespaceType, started.NamespaceID)
	result := []RelationMutation{{Edge: umpire.RelationEdge{
		Type:   WorkflowRunsRelation,
		Scope:  scope,
		Source: scopedRelationEntity(model.WorkflowType, started.NamespaceID, started.WorkflowID),
		Target: run,
	}}}
	if started.PreviousRunID != "" {
		result = append(result, RelationMutation{Edge: umpire.RelationEdge{
			Type:   WorkflowRunSuccessorRelation,
			Scope:  scope,
			Source: scopedRelationEntity(model.WorkflowRunType, started.NamespaceID, started.PreviousRunID),
			Target: run,
		}})
	}
	return result
}

func deriveNexusActivityRelations(observed umpire.Fact) []RelationMutation {
	switch snapshot := observed.(type) {
	case *fact.NexusOperationExecutionSnapshot:
		if snapshot.NamespaceID == "" || snapshot.OperationID == "" {
			return nil
		}
		var result []RelationMutation
		for _, link := range snapshot.Links {
			activity := link.GetActivity()
			if activity == nil || activity.GetActivityId() == "" {
				continue
			}
			result = append(result, RelationMutation{Edge: umpire.RelationEdge{
				Type:   NexusActivityRelation,
				Scope:  umpire.NewEntityID(model.NamespaceType, snapshot.NamespaceID),
				Source: scopedRelationEntity(model.NexusOperationType, snapshot.NamespaceID, snapshot.OperationID),
				Target: scopedRelationEntity(model.ActivityType, snapshot.NamespaceID, activity.GetActivityId()),
			}})
		}
		return result
	case *fact.ActivityExecutionSnapshot:
		if snapshot.NamespaceID == "" || snapshot.ActivityID == "" {
			return nil
		}
		var result []RelationMutation
		for _, link := range snapshot.Links {
			operation := link.GetNexusOperation()
			if operation == nil || operation.GetOperationId() == "" {
				continue
			}
			result = append(result, RelationMutation{Edge: umpire.RelationEdge{
				Type:   ActivityNexusRelation,
				Scope:  umpire.NewEntityID(model.NamespaceType, snapshot.NamespaceID),
				Source: scopedRelationEntity(model.ActivityType, snapshot.NamespaceID, snapshot.ActivityID),
				Target: scopedRelationEntity(model.NexusOperationType, snapshot.NamespaceID, operation.GetOperationId()),
			}})
		}
		return result
	default:
		return nil
	}
}

func scopedRelationEntity(entityType umpire.EntityType, namespaceID, id string) umpire.EntityID {
	return umpire.NewEntityID(entityType, namespaceID+"\x00"+id)
}
