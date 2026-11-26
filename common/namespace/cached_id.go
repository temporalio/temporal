package namespace

// CachedID returns the ID of the namespace with name from the in-memory state of registry. It
// doesn't read through to persistence, so a lookup of a namespace that doesn't exist yet, such as
// for RegisterNamespace, doesn't add the name to the negative lookup cache of the registry. It
// reports false if the registry doesn't have the namespace in memory, which includes a namespace
// that was created very recently.
func CachedID(registry Registry, name Name) (ID, bool) {
	ns, err := registry.GetNamespaceWithOptions(name, GetNamespaceOptions{DisableReadthrough: true})
	if err != nil {
		return "", false
	}
	return ns.ID(), true
}

// CachedName returns the name of the namespace with id from the in-memory state of registry. Like
// CachedID, it doesn't read through to persistence, and it reports false if the registry doesn't
// have the namespace in memory.
func CachedName(registry Registry, id ID) (Name, bool) {
	ns, err := registry.GetNamespaceByIDWithOptions(id, GetNamespaceOptions{DisableReadthrough: true})
	if err != nil {
		return "", false
	}
	return ns.Name(), true
}
