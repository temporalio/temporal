package queues

import (
	"fmt"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
	expmaps "golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ToPersistenceQueueState(
	queueState *queueState,
) *persistencespb.QueueState {
	readerStates := make(map[int64]*persistencespb.QueueReaderState)
	for id, scopes := range queueState.readerScopes {
		persistenceScopes := make([]*persistencespb.QueueSliceScope, 0, len(scopes))
		for _, scope := range scopes {
			persistenceScopes = append(persistenceScopes, ToPersistenceScope(scope))
		}
		readerStates[id] = &persistencespb.QueueReaderState{
			Scopes: persistenceScopes,
		}
	}

	return &persistencespb.QueueState{
		ReaderStates:                 readerStates,
		ExclusiveReaderHighWatermark: ToPersistenceTaskKey(queueState.exclusiveReaderHighWatermark),
	}
}

func FromPersistenceQueueState(
	state *persistencespb.QueueState,
) *queueState {
	readerScopes := make(map[int64][]Scope, len(state.ReaderStates))
	for id, persistenceReaderState := range state.ReaderStates {
		scopes := make([]Scope, 0, len(persistenceReaderState.Scopes))
		for _, persistenceScope := range persistenceReaderState.Scopes {
			scopes = append(scopes, FromPersistenceScope(persistenceScope))
		}
		readerScopes[id] = scopes
	}

	return &queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: FromPersistenceTaskKey(state.ExclusiveReaderHighWatermark),
	}
}

func ToPersistenceScope(
	scope Scope,
) *persistencespb.QueueSliceScope {
	return &persistencespb.QueueSliceScope{
		Range:     ToPersistenceRange(scope.Range),
		Predicate: ToPersistencePredicate(scope.Predicate),
	}
}

func FromPersistenceScope(
	scope *persistencespb.QueueSliceScope,
) Scope {
	return NewScope(
		FromPersistenceRange(scope.Range),
		FromPersistencePredicate(scope.Predicate),
	)
}

func ToPersistenceRange(
	r Range,
) *persistencespb.QueueSliceRange {
	return &persistencespb.QueueSliceRange{
		InclusiveMin: ToPersistenceTaskKey(r.InclusiveMin),
		ExclusiveMax: ToPersistenceTaskKey(r.ExclusiveMax),
	}
}

func FromPersistenceRange(
	r *persistencespb.QueueSliceRange,
) Range {
	return NewRange(
		FromPersistenceTaskKey(r.InclusiveMin),
		FromPersistenceTaskKey(r.ExclusiveMax),
	)
}

func ToPersistenceTaskKey(
	key tasks.Key,
) *persistencespb.TaskKey {
	return &persistencespb.TaskKey{
		FireTime: timestamppb.New(key.FireTime),
		TaskId:   key.TaskID,
	}
}

func FromPersistenceTaskKey(
	key *persistencespb.TaskKey,
) tasks.Key {
	return tasks.NewKey(key.FireTime.AsTime(), key.TaskId)
}

func ToPersistencePredicate(
	predicate tasks.Predicate,
) *persistencespb.Predicate {
	switch predicate := predicate.(type) {
	case *predicates.UniversalImpl[tasks.Task]:
		return ToPersistenceUniversalPredicate(predicate)
	case *predicates.EmptyImpl[tasks.Task]:
		return ToPersistenceEmptyPredicate(predicate)
	case *predicates.AndImpl[tasks.Task]:
		return ToPersistenceAndPredicate(predicate)
	case *predicates.OrImpl[tasks.Task]:
		return ToPersistenceOrPredicate(predicate)
	case *predicates.NotImpl[tasks.Task]:
		return ToPersistenceNotPredicate(predicate)
	case *tasks.NamespacePredicate:
		return ToPersistenceNamespaceIDPredicate(predicate)
	case *tasks.TypePredicate:
		return ToPersistenceTaskTypePredicate(predicate)
	case *tasks.DestinationPredicate:
		return ToPersistenceDestinationPredicate(predicate)
	case *tasks.OutboundTaskGroupPredicate:
		return ToPersistenceOutboundTaskGroupPredicate(predicate)
	case *tasks.OutboundTaskPredicate:
		return ToPersistenceOutboundTaskPredicate(predicate)
	default:
		panic(fmt.Sprintf("unknown task predicate type: %T", predicate))
	}
}

func FromPersistencePredicate(
	predicate *persistencespb.Predicate,
) tasks.Predicate {
	switch predicate.GetPredicateType() {
	case enumsspb.PREDICATE_TYPE_UNIVERSAL:
		return FromPersistenceUniversalPredicate(predicate.GetUniversalPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_EMPTY:
		return FromPersistenceEmptyPredicate(predicate.GetEmptyPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_AND:
		return FromPersistenceAndPredicate(predicate.GetAndPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_OR:
		return FromPersistenceOrPredicate(predicate.GetOrPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_NOT:
		return FromPersistenceNotPredicate(predicate.GetNotPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_NAMESPACE_ID:
		return FromPersistenceNamespaceIDPredicate(predicate.GetNamespaceIdPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_TASK_TYPE:
		return FromPersistenceTaskTypePredicate(predicate.GetTaskTypePredicateAttributes())
	case enumsspb.PREDICATE_TYPE_DESTINATION:
		return FromPersistenceDestinationPredicate(predicate.GetDestinationPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_OUTBOUND_TASK_GROUP:
		return FromPersistenceOutboundTaskGroupPredicate(predicate.GetOutboundTaskGroupPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_OUTBOUND_TASK:
		return FromPersistenceOutboundTaskPredicate(predicate.GetOutboundTaskPredicateAttributes())
	default:
		panic(fmt.Sprintf("unknown persistence task predicate type: %v", predicate.GetPredicateType()))
	}
}

func ToPersistenceUniversalPredicate(
	_ *predicates.UniversalImpl[tasks.Task],
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
		Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
	}
}

func FromPersistenceUniversalPredicate(
	_ *persistencespb.UniversalPredicateAttributes,
) tasks.Predicate {
	return predicates.Universal[tasks.Task]()
}

func ToPersistenceEmptyPredicate(
	_ *predicates.EmptyImpl[tasks.Task],
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_EMPTY,
		Attributes:    &persistencespb.Predicate_EmptyPredicateAttributes{},
	}
}

func FromPersistenceEmptyPredicate(
	_ *persistencespb.EmptyPredicateAttributes,
) tasks.Predicate {
	return predicates.Empty[tasks.Task]()
}

func ToPersistenceAndPredicate(
	andPredicate *predicates.AndImpl[tasks.Task],
) *persistencespb.Predicate {
	persistencePredicates := make([]*persistencespb.Predicate, 0, len(andPredicate.Predicates))
	for _, p := range andPredicate.Predicates {
		persistencePredicates = append(persistencePredicates, ToPersistencePredicate(p))
	}

	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_AND,
		Attributes: &persistencespb.Predicate_AndPredicateAttributes{
			AndPredicateAttributes: &persistencespb.AndPredicateAttributes{
				Predicates: persistencePredicates,
			},
		},
	}
}

func FromPersistenceAndPredicate(
	attributes *persistencespb.AndPredicateAttributes,
) tasks.Predicate {
	taskPredicates := make([]predicates.Predicate[tasks.Task], 0, len(attributes.Predicates))
	for _, p := range attributes.Predicates {
		taskPredicates = append(taskPredicates, FromPersistencePredicate(p))
	}

	return predicates.And(taskPredicates...)
}

func ToPersistenceOrPredicate(
	orPredicate *predicates.OrImpl[tasks.Task],
) *persistencespb.Predicate {
	persistencePredicates := make([]*persistencespb.Predicate, 0, len(orPredicate.Predicates))
	for _, p := range orPredicate.Predicates {
		persistencePredicates = append(persistencePredicates, ToPersistencePredicate(p))
	}

	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_OR,
		Attributes: &persistencespb.Predicate_OrPredicateAttributes{
			OrPredicateAttributes: &persistencespb.OrPredicateAttributes{
				Predicates: persistencePredicates,
			},
		},
	}
}

func FromPersistenceOrPredicate(
	attributes *persistencespb.OrPredicateAttributes,
) tasks.Predicate {
	taskPredicates := make([]predicates.Predicate[tasks.Task], 0, len(attributes.Predicates))
	for _, p := range attributes.Predicates {
		taskPredicates = append(taskPredicates, FromPersistencePredicate(p))
	}

	return predicates.Or(taskPredicates...)
}

func ToPersistenceNotPredicate(
	notPredicate *predicates.NotImpl[tasks.Task],
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_NOT,
		Attributes: &persistencespb.Predicate_NotPredicateAttributes{
			NotPredicateAttributes: &persistencespb.NotPredicateAttributes{
				Predicate: ToPersistencePredicate(notPredicate.Predicate),
			},
		},
	}
}

func FromPersistenceNotPredicate(
	attributes *persistencespb.NotPredicateAttributes,
) tasks.Predicate {
	return predicates.Not(FromPersistencePredicate(attributes.Predicate))
}

func ToPersistenceNamespaceIDPredicate(
	namespaceIDPredicate *tasks.NamespacePredicate,
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_NAMESPACE_ID,
		Attributes: &persistencespb.Predicate_NamespaceIdPredicateAttributes{
			NamespaceIdPredicateAttributes: &persistencespb.NamespaceIdPredicateAttributes{
				NamespaceIds: expmaps.Keys(namespaceIDPredicate.NamespaceIDs),
			},
		},
	}
}

func FromPersistenceNamespaceIDPredicate(
	attributes *persistencespb.NamespaceIdPredicateAttributes,
) tasks.Predicate {
	return tasks.NewNamespacePredicate(attributes.NamespaceIds)
}

func ToPersistenceTaskTypePredicate(
	taskTypePredicate *tasks.TypePredicate,
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_TASK_TYPE,
		Attributes: &persistencespb.Predicate_TaskTypePredicateAttributes{
			TaskTypePredicateAttributes: &persistencespb.TaskTypePredicateAttributes{
				TaskTypes: expmaps.Keys(taskTypePredicate.Types),
			},
		},
	}
}

func FromPersistenceTaskTypePredicate(
	attributes *persistencespb.TaskTypePredicateAttributes,
) tasks.Predicate {
	return tasks.NewTypePredicate(attributes.TaskTypes)
}

func ToPersistenceDestinationPredicate(
	taskDestinationPredicate *tasks.DestinationPredicate,
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_DESTINATION,
		Attributes: &persistencespb.Predicate_DestinationPredicateAttributes{
			DestinationPredicateAttributes: &persistencespb.DestinationPredicateAttributes{
				Destinations: expmaps.Keys(taskDestinationPredicate.Destinations),
			},
		},
	}
}

func FromPersistenceDestinationPredicate(
	attributes *persistencespb.DestinationPredicateAttributes,
) tasks.Predicate {
	return tasks.NewDestinationPredicate(attributes.Destinations)
}

func ToPersistenceOutboundTaskGroupPredicate(
	pred *tasks.OutboundTaskGroupPredicate,
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_OUTBOUND_TASK_GROUP,
		Attributes: &persistencespb.Predicate_OutboundTaskGroupPredicateAttributes{
			OutboundTaskGroupPredicateAttributes: &persistencespb.OutboundTaskGroupPredicateAttributes{
				Groups: expmaps.Keys(pred.Groups),
			},
		},
	}
}

func FromPersistenceOutboundTaskGroupPredicate(
	attributes *persistencespb.OutboundTaskGroupPredicateAttributes,
) tasks.Predicate {
	return tasks.NewOutboundTaskGroupPredicate(attributes.Groups)
}

func ToPersistenceOutboundTaskPredicate(
	pred *tasks.OutboundTaskPredicate,
) *persistencespb.Predicate {
	groups := make([]*persistencespb.OutboundTaskPredicateAttributes_Group, 0, len(pred.Groups))
	for g := range pred.Groups {
		groups = append(groups, &persistencespb.OutboundTaskPredicateAttributes_Group{
			TaskGroup:   g.TaskGroup,
			NamespaceId: g.NamespaceID,
			Destination: g.Destination,
		})
	}

	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_OUTBOUND_TASK,
		Attributes: &persistencespb.Predicate_OutboundTaskPredicateAttributes{
			OutboundTaskPredicateAttributes: &persistencespb.OutboundTaskPredicateAttributes{
				Groups: groups,
			},
		},
	}
}

func FromPersistenceOutboundTaskPredicate(
	attributes *persistencespb.OutboundTaskPredicateAttributes,
) tasks.Predicate {
	groups := make([]tasks.TaskGroupNamespaceIDAndDestination, len(attributes.Groups))
	for i, g := range attributes.Groups {
		groups[i] = tasks.TaskGroupNamespaceIDAndDestination{
			TaskGroup:   g.TaskGroup,
			NamespaceID: g.NamespaceId,
			Destination: g.Destination,
		}
	}
	return tasks.NewOutboundTaskPredicate(groups)
}
