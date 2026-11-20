package frontend

import (
	"context"
)

// checkNamespaceRepairPolicy refuses operator repair of a namespace the
// deployment's repair policy does not support. A nil policy allows every namespace.
func (adh *AdminHandler) checkNamespaceRepairPolicy(
	ctx context.Context,
	namespaceID string,
	namespaceName string,
	data map[string]string,
) error {
	if adh.repairPolicy == nil {
		return nil
	}
	return adh.repairPolicy(ctx, namespaceID, namespaceName, data)
}
