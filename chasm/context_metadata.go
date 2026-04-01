package chasm

// ContextMetadataProvider if implemented by the root Component, allows the CHASM
// framework to export execution metadata into the request context. This is typically used for
// observability use cases eg. metrics requiring high cardinality for metering.
type ContextMetadataProvider interface {
	ContextMetadata(Context) map[string]string
}
