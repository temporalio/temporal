package migration

import (
	"encoding/json"
)

type ExecutionInfo struct {
	BusinessID  string `json:"business_id,omitempty"`
	RunID       string `json:"run_id,omitempty"`
	ArchetypeID uint32 `json:"archetype_id,omitempty"`
}

type executionInfoLegacyJSON struct {
	BusinessID  string `json:"business_id,omitempty"`
	WorkflowID  string `json:"workflow_id,omitempty"`
	RunID       string `json:"run_id,omitempty"`
	ArchetypeID uint32 `json:"archetype_id,omitempty"`
}

func (e *ExecutionInfo) UnmarshalJSON(data []byte) error {
	// For backward compatibility, support both workflow_id and business_id here.
	var legacy executionInfoLegacyJSON
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}

	businessID := legacy.WorkflowID
	if legacy.BusinessID != "" {
		businessID = legacy.BusinessID
	}

	e.BusinessID = businessID
	e.RunID = legacy.RunID
	e.ArchetypeID = legacy.ArchetypeID
	return nil
}
