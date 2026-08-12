package fact

import "go.temporal.io/server/common/testing/umpire"

// nsPath builds a namespace-rooted EntityPath for self, nested beneath the given
// intermediate parents (root-first, namespace excluded). A blank namespaceID
// yields an unrooted path (self plus parents only).
func nsPath(namespaceID string, self umpire.EntityID, parents ...umpire.EntityID) *umpire.EntityPath {
	ancestors := make([]umpire.EntityID, 0, len(parents)+1)
	if namespaceID != "" {
		ancestors = append(ancestors, umpire.NewEntityID(NamespaceType, namespaceID))
	}
	ancestors = append(ancestors, parents...)
	return &umpire.EntityPath{EntityID: self, Ancestors: ancestors}
}
