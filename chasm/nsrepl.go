package chasm

const (
	NamespaceReplicationLibraryName   = "nsrepl"
	NamespaceReplicationComponentName = "namespace_mutation"
)

var (
	NamespaceReplicationComponentID = GenerateTypeID(FullyQualifiedName(NamespaceReplicationLibraryName, NamespaceReplicationComponentName))
)
