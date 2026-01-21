package migration

import (
	"encoding/json"

	"go.temporal.io/server/common/dynamicconfig"
	"go.uber.org/fx"
)

var ExecutionInfoEncodingModule = fx.Options(
	fx.Provide(func(
		dynamicCollection *dynamicconfig.Collection,
	) enableExecutionInfoNewJSONEncoding {
		return enableExecutionInfoNewJSONEncoding(dynamicconfig.WorkerEnableMigrationExecutionInfoNewJSONEncoding.Get(dynamicCollection)())
	}),
	fx.Populate(&executionInfoNewJSONEncodingEnabled),
)

type enableExecutionInfoNewJSONEncoding bool

// executionInfoNewJSONEncodingEnabled can be removed in v1.31 when both released OSS and cloud version
// support decoding "businessID".
var executionInfoNewJSONEncodingEnabled enableExecutionInfoNewJSONEncoding

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
	if executionInfoNewJSONEncodingEnabled {
		return json.Marshal(e.executionInfoNewJSON)
	}

	return json.Marshal(executionInfoLegacyJSON{
		WorkflowID:  e.BusinessID,
		RunID:       e.RunID,
		ArchetypeID: e.ArchetypeID,
	})
}

func (e *ExecutionInfo) UnmarshalJSON(data []byte) error {
	if executionInfoNewJSONEncodingEnabled {
		return json.Unmarshal(data, &e.executionInfoNewJSON)
	}

	// For forward compatibility, support both workflow_id and business_id here.
	// Then in v1.31, we can always encode using "business_id" and also support downgrade.
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
