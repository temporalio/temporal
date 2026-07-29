// Package nexusworkflowref converts a Nexus operation completion token between its HSM and CHASM
// forms. A rebuild (reset or conflict resolution) can re-create a workflow's Nexus operation under
// the other framework by dynamic config after its callback token was minted, so a completion may
// arrive addressing the framework the operation no longer lives in; converting the token lets it be
// retried against the current framework. Only workflow-backed operations are convertible — the HSM
// operation state machine and the CHASM workflow's Operations map are two representations of the
// same thing, keyed by scheduled event ID.
package nexusworkflowref

import (
	"errors"
	"fmt"
	"strconv"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/components/nexusoperations"
)

// operationsFieldName is the first segment of a workflow Nexus operation's CHASM component path (the
// scheduled event ID, the map key, is the second). It must stay in sync with the Workflow.Operations
// field in chasm/lib/workflow.
const operationsFieldName = "Operations"

// operationIdentity is the framework-agnostic identity of a workflow Nexus operation: the fields
// shared by the HSM and CHASM completion refs, sufficient to address the operation in either.
type operationIdentity struct {
	namespaceID      string
	workflowID       string
	runID            string
	scheduledEventID int64
	requestID        string
}

// HSMRefToCHASMRef converts an HSM-format Nexus operation completion token into the equivalent
// CHASM-format token.
func HSMRefToCHASMRef(completion *tokenspb.NexusOperationCompletion) (*tokenspb.NexusOperationCompletion, error) {
	id, err := identityFromHSM(completion)
	if err != nil {
		return nil, err
	}
	return chasmCompletion(id)
}

// CHASMRefToHSMRef converts a CHASM-format Nexus operation completion token into the equivalent
// HSM-format token. It errors if the CHASM ref is not a workflow-backed operation (wrong archetype
// or unexpected component path), since only those have an HSM equivalent.
func CHASMRefToHSMRef(completion *tokenspb.NexusOperationCompletion) (*tokenspb.NexusOperationCompletion, error) {
	id, err := identityFromCHASM(completion)
	if err != nil {
		return nil, err
	}
	return hsmCompletion(id), nil
}

func identityFromHSM(completion *tokenspb.NexusOperationCompletion) (operationIdentity, error) {
	scheduledEventID, err := scheduledEventIDFromHSMRef(completion.GetRef())
	if err != nil {
		return operationIdentity{}, err
	}
	return operationIdentity{
		namespaceID:      completion.GetNamespaceId(),
		workflowID:       completion.GetWorkflowId(),
		runID:            completion.GetRunId(),
		scheduledEventID: scheduledEventID,
		requestID:        completion.GetRequestId(),
	}, nil
}

func identityFromCHASM(completion *tokenspb.NexusOperationCompletion) (operationIdentity, error) {
	var ref persistencespb.ChasmComponentRef
	if err := ref.Unmarshal(completion.GetComponentRef()); err != nil {
		return operationIdentity{}, fmt.Errorf("failed to unmarshal component ref: %w", err)
	}
	// Only a workflow-backed operation has an HSM equivalent. Confirm the ref addresses one via the
	// workflow archetype and the Operations component path before treating it as an operation.
	if ref.GetArchetypeId() != chasm.WorkflowArchetypeID {
		return operationIdentity{}, fmt.Errorf("component ref is not workflow-backed (archetype %d)", ref.GetArchetypeId())
	}
	scheduledEventID, err := scheduledEventIDFromComponentPath(ref.GetComponentPath())
	if err != nil {
		return operationIdentity{}, err
	}
	return operationIdentity{
		namespaceID:      ref.GetNamespaceId(),
		workflowID:       ref.GetBusinessId(),
		runID:            ref.GetRunId(),
		scheduledEventID: scheduledEventID,
		requestID:        completion.GetRequestId(),
	}, nil
}

// chasmCompletion builds a CHASM-format completion token. The ComponentRef carries no versioned
// transitions, so identity is re-established by request ID; the consistency level chosen by the
// resolver then selects the run (ComponentCreation keeps the ref's run, CurrentRun the current run).
func chasmCompletion(id operationIdentity) (*tokenspb.NexusOperationCompletion, error) {
	ref, err := (&persistencespb.ChasmComponentRef{
		NamespaceId:   id.namespaceID,
		BusinessId:    id.workflowID,
		RunId:         id.runID,
		ArchetypeId:   chasm.WorkflowArchetypeID,
		ComponentPath: []string{operationsFieldName, strconv.FormatInt(id.scheduledEventID, 10)},
	}).Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal component ref: %w", err)
	}
	return &tokenspb.NexusOperationCompletion{
		ComponentRef: ref,
		RequestId:    id.requestID,
	}, nil
}

// hsmCompletion builds an HSM-format completion token. The versioned-transition fields are non-nil
// zero values: the completion handler's reset fallback zeroes their transition counts and would
// nil-panic if they were unset. Identity is re-established by request ID.
func hsmCompletion(id operationIdentity) *tokenspb.NexusOperationCompletion {
	return &tokenspb.NexusOperationCompletion{
		NamespaceId: id.namespaceID,
		WorkflowId:  id.workflowID,
		RunId:       id.runID,
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{{
				Type: nexusoperations.OperationMachineType,
				Id:   strconv.FormatInt(id.scheduledEventID, 10),
			}},
			MachineInitialVersionedTransition:    &persistencespb.VersionedTransition{},
			MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{},
		},
		RequestId: id.requestID,
	}
}

func scheduledEventIDFromHSMRef(ref *persistencespb.StateMachineRef) (int64, error) {
	for _, key := range ref.GetPath() {
		if key.GetType() != nexusoperations.OperationMachineType {
			continue
		}
		scheduledEventID, err := strconv.ParseInt(key.GetId(), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid operation state machine id %q: %w", key.GetId(), err)
		}
		return scheduledEventID, nil
	}
	return 0, errors.New("completion token has no operation state machine reference")
}

func scheduledEventIDFromComponentPath(path []string) (int64, error) {
	if len(path) != 2 || path[0] != operationsFieldName {
		return 0, fmt.Errorf("unexpected nexus operation component path %v", path)
	}
	scheduledEventID, err := strconv.ParseInt(path[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid scheduled event id %q: %w", path[1], err)
	}
	return scheduledEventID, nil
}
