package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type Config struct {
	chasm.UnimplementedComponent

	*activitypb.Identifier // Needed for partial read lookup?
	*activitypb.Config

	Activity chasm.Field[*Activity]
}
