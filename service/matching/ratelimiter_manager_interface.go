package matching

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/quotas"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type (
	// RateLimiterManager interface defines methods to select the appropriate rate limiter for task queues.
	rateLimitManager interface {
		computeEffectiveRPSAndSource(taskDispatchRate float64)

		// SetWorkerRPS : Injects the worker set rate limit from the poll metadata lazily into rate limit manager.
		// Allowing the rate limit manager to use the worker set RPS when available.
		SetWorkerRPS(meta *pollMetadata)

		// SelectRateLimiter : Decides which RPS to set for the dynamicRateLimiter.
		// If task queue config is set via api, it uses the RPS persisted in the user data.
		// If not set, it checks for worker set RPS. If task queue config is set via worker,
		// it uses the RPS from in-memory set via TaskQueueActivitiesPreSecond at the time of worker deployment.
		// If no RPS is set, it uses the default task dispatch RPS.
		// Returns the rate limit that needs to be configured and a boolean indicating whether an update is needed.
		SelectTaskQueueRateLimiter(tqType enumspb.TaskQueueType) (*wrapperspb.DoubleValue, bool)

		// TrySetRPSFromUserData : Attempts to set effectiveRPS from user data.
		// Returns true if RPS was set from user data, false otherwise.
		TrySetRPSFromUserData(tqType enumspb.TaskQueueType) bool

		GetSourceForEffectiveRPS() enumspb.RateLimitSource

		GetEffectiveRPS(tqType enumspb.TaskQueueType) float64

		GetRateLimiter() quotas.RateLimiter

		SetSourceForEffectiveRPS(source enumspb.RateLimitSource)

		UpdateRatelimit(rps float64)
	}
)
