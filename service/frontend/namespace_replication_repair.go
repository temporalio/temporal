package frontend

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
)

// isOperatorRepair reports whether a namespace mutation is an operator repair,
// which the repair policy guards.
func isOperatorRepair(request *adminservice.ApplyNamespaceMutationRequest) bool {
	return request.GetAttribution().GetOrigin() == adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR
}

// validateNamespaceMutationAttribution rejects an ApplyNamespaceMutation whose
// attribution is malformed, before its payload is read.
func validateNamespaceMutationAttribution(request *adminservice.ApplyNamespaceMutationRequest) error {
	attribution := request.GetAttribution()
	if attribution == nil {
		return nil
	}
	switch attribution.GetOrigin() {
	case adminservice.NamespaceMutationAttribution_ORIGIN_UNSPECIFIED:
		if attribution.GetRepairId() != "" || attribution.GetReason() != "" {
			return serviceerror.NewInvalidArgument("namespace mutation attribution origin is required")
		}
		return nil
	case adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR:
		if request.GetShadow() {
			return serviceerror.NewInvalidArgument("operator repair cannot use shadow apply")
		}
		if attribution.GetRepairId() == "" || attribution.GetReason() == "" {
			return serviceerror.NewInvalidArgument("operator repair attribution requires repair_id and reason")
		}
		return nil
	default:
		return serviceerror.NewInvalidArgument("unknown namespace mutation attribution origin")
	}
}

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
