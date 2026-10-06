package common

import "go.temporal.io/server/common/primitives"

// WorkflowIDToHistoryShard remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.WorkflowIDToHistoryShard.
var WorkflowIDToHistoryShard = primitives.WorkflowIDToHistoryShard

// ScheduledTaskMinPrecision remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.ScheduledTaskMinPrecision.
const ScheduledTaskMinPrecision = primitives.ScheduledTaskMinPrecision
