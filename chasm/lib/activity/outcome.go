package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type Outcome struct {
	chasm.UnimplementedComponent

	*activitypb.Identifier // Needed for partial read lookup?
	*activitypb.Outcome

	Activity chasm.Field[*Activity]
}
