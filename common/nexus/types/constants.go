package types

// CallbackTokenHeader is the HTTP header name for the Nexus callback token.
// This is a shared constant used across packages that need to reference
// the callback token header without importing the full nexus package.
const CallbackTokenHeader = "Temporal-Callback-Token"
