package nexus

// SystemCallbackURL is the reserved callback URL used to route Nexus operation callbacks
// internally within Temporal. It must match the scheme/host used in validation and routing logic.
const SystemCallbackURL = "temporal://system"

// SystemEndpoint is the reserved endpoint name for Temporal system operations.
// Operation requests for this endpoint are routed internally within the history service.
const SystemEndpoint = "__temporal_system"

// SystemPayloadMetadataKey is the Payload metadata key set to "true", on every
// Temporal-generated system Nexus response payload.
const SystemPayloadMetadataKey = "__temporal_system_payload"
