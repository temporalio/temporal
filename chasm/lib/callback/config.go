package callback

import (
	"errors"
	"fmt"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/backoff"
	commoncallbacks "go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
)

var MaxPerExecution = dynamicconfig.NewNamespaceIntSetting(
	"callback.maxPerExecution",
	2000,
	`MaxPerExecution is the maximum number of callbacks that can be attached to an execution (workflow or standalone activity).`,
)

// TODO(chrsmith): This just caps the size of an individual source context payload.
// We also need to wire through an aggregate max size, for all callbacks in an execution.
// (We expect that users will want fewer NexusHandler callbacks with larger payloads than the
// full 2k execution callbacks, with a much smaller per-callback payload size.)

var NexusHandlerSourceContextMaxSize = dynamicconfig.NewNamespaceIntSetting(
	"callback.nexusHandler.sourceContext.maxSize",
	1024*1024,
	`The maximum allowed size, in bytes, of the opaque source context attached to a single NexusHandler
completion callback. The server carries this payload to the callback's handler untouched.`,
)

var RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
	"callback.request.timeout",
	time.Second*10,
	`RequestTimeout is the timeout for executing a single callback request.`,
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"callback.retryPolicy.initialInterval",
	time.Second,
	`The initial backoff interval between every callback request attempt for a given callback.`,
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"callback.retryPolicy.maxInterval",
	time.Hour,
	`The maximum backoff interval between every callback request attempt for a given callback.`,
)

var InspectSourceHeader = dynamicconfig.NewGlobalBoolSetting(
	"callback.inspectSourceHeader",
	false,
	`Controls whether the legacy "source" header should be inspected to determine if a Nexus callback request is internal
or external. This header was used before worker callbacks used the temporal://system URL. Leave this disabled unless it
is required for mixed-version compatibility because trusting a caller-controlled header can route external requests
internally.`,
)

type Config struct {
	RequestTimeout                           dynamicconfig.DurationPropertyFnWithDestinationFilter
	RetryPolicy                              dynamicconfig.TypedPropertyFn[backoff.RetryPolicy]
	InspectSourceHeader                      dynamicconfig.BoolPropertyFn
	InternalCallbackCrossNamespaceArchetypes dynamicconfig.TypedPropertyFn[[]string]
}

func configProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		RequestTimeout:                           RequestTimeout.Get(dc),
		InternalCallbackCrossNamespaceArchetypes: InternalCallbackCrossNamespaceArchetypes.Get(dc),
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(
				RetryPolicyInitialInterval.Get(dc)(),
			).WithMaximumInterval(
				RetryPolicyMaximumInterval.Get(dc)(),
			).WithExpirationInterval(
				backoff.NoInterval,
			)
		},
		InspectSourceHeader: InspectSourceHeader.Get(dc),
	}
}

var InternalCallbackCrossNamespaceArchetypes = dynamicconfig.NewGlobalTypedSetting(
	"callback.internal.crossNamespaceArchetypes",
	[]string(nil),
	`The list of fully-qualified CHASM archetype names whose internal callbacks may target a namespace other than the
callback source namespace. Internal callbacks for all other archetypes must target the source namespace. Only add an
archetype here as an escape hatch; cross-namespace internal callbacks are not expected.`,
)

var (
	ErrInvalidInternalCallbackRef        = errors.New("invalid CHASM ComponentRef")
	ErrInternalCallbackNamespaceMismatch = errors.New("internal callback namespace mismatch")
)

// ValidateInternalCallbackRef checks that a temporal://internal callback's component ref is well formed and targets
// the callback's source namespace, unless its archetype is in crossNamespaceArchetypes. The token is caller supplied,
// so this must run on every internal delivery path (CHASM and HSM callbacks) before calling History.
func ValidateInternalCallbackRef(
	serializedRef []byte,
	sourceNamespaceID namespace.ID,
	crossNamespaceArchetypes []string,
) error {
	ref := &persistencespb.ChasmComponentRef{}
	if err := ref.Unmarshal(serializedRef); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidInternalCallbackRef, err)
	}
	if ref.GetNamespaceId() == "" || ref.GetBusinessId() == "" {
		return ErrInvalidInternalCallbackRef
	}
	if ref.GetNamespaceId() == sourceNamespaceID.String() {
		return nil
	}
	for _, archetype := range crossNamespaceArchetypes {
		if chasm.GenerateTypeID(archetype) == ref.GetArchetypeId() {
			return nil
		}
	}
	return ErrInternalCallbackNamespaceMismatch
}

var EncodeInternalTokenWithEnvelope = dynamicconfig.NewNamespaceBoolSetting(
	"callback.encodeInternalTokenWithEnvelope",
	false,
	`Controls how the internal CHASM Nexus completion callback token is encoded. When true the token is
encoded as a NexusOperationCompletion envelope; when false (default) it is the legacy bare base64-encoded
ChasmComponentRef. Gates a safe fleet-wide rollout of the envelope encoding: keep disabled until every
server can read it (any server able to read the envelope also accepts the legacy form), then enable
per-namespace.`,
)

var AllowedAddresses = dynamicconfig.NewNamespaceTypedSettingWithConverter(
	"callback.allowedAddresses",
	commoncallbacks.AllowedAddressConverter,
	commoncallbacks.AddressMatchRules{},
	`The per-namespace list of addresses that are allowed for callbacks and whether secure connections (https) are required.
URLs: "temporal://system" and "temporal://internal" are always allowed. The default is no address rules.
URLs are checked against each in order when starting a workflow or activitiy with attached callbacks or a standalone
callback and only need to match one to pass validation.  This configuration is required for external endpoint targets;
any invalid entries are ignored. Each entry is a map with possible values:
     - "Pattern":string (required) the host:port pattern to which this config applies.
        Wildcards, '*', are supported and can match any number of characters (e.g. '*' matches everything,
        'prefix.*.domain' matches 'prefix.a.domain' as well as 'prefix.a.b.domain').
     - "AllowInsecure":bool (optional, default=false) indicates whether https is required`)
