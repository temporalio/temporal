package nexus

// SystemCallbackURL is the reserved callback URL used to route Nexus operation callbacks
// internally within Temporal. It must match the scheme/host used in validation and routing logic.
const SystemCallbackURL = "temporal://system"

// SystemEndpoint is the reserved endpoint name for Temporal system operations.
// Operation requests for this endpoint are routed internally within the history service.
const SystemEndpoint = "__temporal_system"

// SystemEndpointPayloadMetadataKey is the Payload metadata key set, with a value of "true", on the
// response Payload of a SystemEndpoint Nexus operation whose result type embeds a nested
// Payload/Payloads field. The nested Payload's bytes are hidden inside the outer Payload's opaque
// Data, so this flag tells downstream consumers they must unwrap the outer Payload before any
// further Payload processing (e.g. codec decoding) can reach what's inside. It is only ever set for
// SystemEndpoint operations; it is absent (equivalent to false) in all other cases.
const SystemEndpointPayloadMetadataKey = "__temporalSystemNexusEndpoint"
