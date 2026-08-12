// Package capability defines typed environment requirements for sparse regression plans.
package capability

import "go.temporal.io/server/common/testing/umpire/regress"

var (
	CHASM             = regress.RequirementSchema("CHASM")
	ActivityCallbacks = regress.RequirementSchema("ActivityCallbacks")
	Faults            = regress.RequirementSchema("Faults")
)
