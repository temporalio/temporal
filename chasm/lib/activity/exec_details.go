package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type ExecDetails struct {
	chasm.UnimplementedComponent

	*activitypb.Identifier // Needed for partial read lookup?
	*activitypb.ExecutionDetails

	ExecInfo chasm.Field[*ExecInfo]
	Activity chasm.Field[*Activity]
}
