package chasm

import (
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/definition"
)

type InternalKeyConverter interface {
	ToInternalKey(key EntityKey, archetype string) (definition.WorkflowKey, error)
	FromInternalKey(internalKey definition.WorkflowKey, archetype string) (EntityKey, error)
}

const defaultArchetypeSeparator = ":"

// TODO: Consider inject the converter via FX.
var DefaultInternalKeyConverter = &defaultInternalKeyConverter{}

var _ InternalKeyConverter = (*defaultInternalKeyConverter)(nil)

type defaultInternalKeyConverter struct{}

// TODO: special handle the internal key for Scheduler to match existing Scheduler's WorkflowID.

func (c *defaultInternalKeyConverter) ToInternalKey(
	key EntityKey,
	archetype string,
) (definition.WorkflowKey, error) {
	return definition.WorkflowKey{
		NamespaceID: key.NamespaceID,
		WorkflowID:  archetype + defaultArchetypeSeparator + key.BusinessID,
		RunID:       key.EntityID,
	}, nil
}

func (c *defaultInternalKeyConverter) FromInternalKey(
	internalKey definition.WorkflowKey,
	archetype string,
) (EntityKey, error) {
	internalID := internalKey.WorkflowID
	if !strings.HasPrefix(internalID, archetype+defaultArchetypeSeparator) {
		return EntityKey{}, serviceerror.NewInternalf("invalid internal ID: %s", internalID)

	}

	return EntityKey{
		NamespaceID: internalKey.NamespaceID,
		BusinessID:  internalKey.WorkflowID[len(archetype)+1:],
		EntityID:    internalKey.RunID,
	}, nil
}
