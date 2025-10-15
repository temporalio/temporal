package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type Heartbeat struct {
	chasm.UnimplementedComponent

	*activitypb.Identifier // Needed for partial read lookup?
	*activitypb.Heartbeat

	Activity chasm.Field[*Activity]
}
