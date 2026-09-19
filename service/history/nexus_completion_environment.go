package history

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	historyi "go.temporal.io/server/service/history/interfaces"
)

type currentRunNexusOperationAccessor interface {
	accessCurrentNexusOperation(context.Context, hsm.Ref, string, func(*hsm.Node) error) error
}

// accessCurrentNexusOperation looks up a Nexus operation on the current run by path and request ID,
// then applies accessor while holding the workflow lock. This is a migration-only compatibility path
// for a CHASM completion token whose operation was rebuilt into HSM after reset. It intentionally does
// not participate in general HSM reference validation.
func (e *stateMachineEnvironment) accessCurrentNexusOperation(
	ctx context.Context,
	ref hsm.Ref,
	requestID string,
	accessor func(*hsm.Node) error,
) error {
	if ref.WorkflowKey.RunID != "" || requestID == "" {
		return serviceerror.NewNotFound("operation not found")
	}

	return e.access(
		ctx,
		ref,
		hsm.AccessWrite,
		func(_ historyi.WorkflowContext, ms historyi.MutableState, potentialStaleState bool) error {
			if err := e.validateNotZombieWorkflow(ms, hsm.AccessWrite); err != nil {
				return err
			}
			node, err := ms.HSM().Child(ref.StateMachinePath())
			if err != nil {
				if errors.Is(err, hsm.ErrStateMachineNotFound) {
					if potentialStaleState {
						return fmt.Errorf("%w: %w", consts.ErrStaleState, err)
					}
					return serviceerror.NewNotFound("operation not found")
				}
				return fmt.Errorf("%w: %w", serviceerror.NewInternal("node lookup failed"), err)
			}
			operation, err := hsm.MachineData[nexusoperations.Operation](node)
			if err != nil || operation.RequestId != requestID {
				return serviceerror.NewNotFound("operation not found")
			}
			return nil
		},
		accessor,
	)
}
