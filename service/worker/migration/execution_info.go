package migration

import (
	"encoding/json"

	"go.temporal.io/server/common/dynamicconfig"
	"go.uber.org/fx"
)

var ExecutionInfoEncodingModule = fx.Options(
	fx.Provide(func(
		dynamicCollection *dynamicconfig.Collection,
	) EnableExecutionInfoNewJSONEncoding {
		return EnableExecutionInfoNewJSONEncoding(dynamicconfig.WorkerEnableMigrationExecutionInfoNewJSONEncoding.Get(dynamicCollection)())
	}),
	fx.Populate(ExecutionInfoNewJSONEncodingEnabled),
)

type EnableExecutionInfoNewJSONEncoding bool

var ExecutionInfoNewJSONEncodingEnabled EnableExecutionInfoNewJSONEncoding

type ExecutionInfo struct {
	executionInfoNewJSON
}

type executionInfoNewJSON struct {
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

func (e *ExecutionInfo) MarshalJSON() ([]byte, error) {
	if ExecutionInfoNewJSONEncodingEnabled {
		return json.Marshal(e.executionInfoNewJSON)
	}

	return json.Marshal(executionInfoLegacyJSON{
		WorkflowID:  e.BusinessID,
		RunID:       e.RunID,
		ArchetypeID: e.ArchetypeID,
	})
}

func (e *ExecutionInfo) UnmarshalJSON(data []byte) error {
	if ExecutionInfoNewJSONEncodingEnabled {
		return json.Unmarshal(data, &e.executionInfoNewJSON)
	}

	// For forward compatibility, support both workflow_id and business_id here
	var legacy executionInfoLegacyJSON
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}

	businessID := legacy.WorkflowID
	if legacy.BusinessID != "" {
		businessID = legacy.BusinessID
	}

	e.executionInfoNewJSON = executionInfoNewJSON{
		BusinessID:  businessID,
		RunID:       legacy.RunID,
		ArchetypeID: legacy.ArchetypeID,
	}
	return nil
}
