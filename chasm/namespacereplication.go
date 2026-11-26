package chasm

const (
	NamespaceReplicationLibraryName   = "namespacereplication"
	NamespaceReplicationComponentName = "namespacereplication"
)

var (
	NamespaceReplicationComponentID = GenerateTypeID(FullyQualifiedName(NamespaceReplicationLibraryName, NamespaceReplicationComponentName))
)
