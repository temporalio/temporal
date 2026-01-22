package tdbg

// Import chasm lib protobuf packages for side effects (protobuf type registration).
// This allows tdbg decode to find protobuf types from chasm/lib packages.
//nolint:revive
import (
	_ "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	_ "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	_ "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	_ "go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
)
