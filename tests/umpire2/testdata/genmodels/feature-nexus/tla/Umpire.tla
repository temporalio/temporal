---- MODULE Umpire ----
EXTENDS FiniteSets, Naturals

CONSTANTS
    \* @type: Set(Str);
    NexusOperationIDs,
    \* @type: Set(Str);
    NexusTimeoutEvidenceIDs,
    \* @type: Set(Str);
    WorkflowIDs

VARIABLES
    \* @type: Set(Str);
    exists_NexusOperation,
    \* @type: Str -> Str;
    state_NexusOperation,
    \* @type: Set(Str);
    exists_NexusTimeoutEvidence,
    \* @type: Str -> Str;
    state_NexusTimeoutEvidence,
    \* @type: Set(Str);
    exists_Workflow,
    \* @type: Str -> Str;
    state_Workflow,
    \* @type: Set(<<Str, Str>>);
    relation_nexus_operation_workflow,
    \* @type: Set(<<Str, Str>>);
    relation_nexus_timeout_evidence

vars == <<exists_NexusOperation, state_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

TypeOK ==
    /\ exists_NexusOperation \in SUBSET NexusOperationIDs
    /\ state_NexusOperation \in [NexusOperationIDs -> {"backing_off", "canceled", "failed", "rejected", "scheduled", "started", "succeeded", "terminated", "timed_out", "unspecified"}]
    /\ exists_NexusTimeoutEvidence \in SUBSET NexusTimeoutEvidenceIDs
    /\ state_NexusTimeoutEvidence \in [NexusTimeoutEvidenceIDs -> {"invalid", "unobserved", "valid"}]
    /\ exists_Workflow \in SUBSET WorkflowIDs
    /\ state_Workflow \in [WorkflowIDs -> {"canceled", "completed", "created", "failed", "started", "terminated", "timed_out"}]
    /\ relation_nexus_operation_workflow \in SUBSET (NexusOperationIDs \X WorkflowIDs)
    /\ relation_nexus_timeout_evidence \in SUBSET (NexusOperationIDs \X NexusTimeoutEvidenceIDs)

Cardinality_nexus_operation_workflow ==
    /\ \A tuple \in relation_nexus_operation_workflow: tuple[1] \in exists_NexusOperation /\ tuple[2] \in exists_Workflow
    /\ \A source \in NexusOperationIDs: Cardinality({target \in WorkflowIDs: <<source, target>> \in relation_nexus_operation_workflow}) <= 1

Cardinality_nexus_timeout_evidence ==
    /\ \A tuple \in relation_nexus_timeout_evidence: tuple[1] \in exists_NexusOperation /\ tuple[2] \in exists_NexusTimeoutEvidence
    /\ \A source \in NexusOperationIDs: Cardinality({target \in NexusTimeoutEvidenceIDs: <<source, target>> \in relation_nexus_timeout_evidence}) <= 1
    /\ \A target \in NexusTimeoutEvidenceIDs: Cardinality({source \in NexusOperationIDs: <<source, target>> \in relation_nexus_timeout_evidence}) <= 1

Init ==
    /\ exists_NexusOperation = {}
    /\ state_NexusOperation = [entity \in NexusOperationIDs |-> "unspecified"]
    /\ exists_NexusTimeoutEvidence = {}
    /\ state_NexusTimeoutEvidence = [entity \in NexusTimeoutEvidenceIDs |-> "unobserved"]
    /\ exists_Workflow = {}
    /\ state_Workflow = [entity \in WorkflowIDs |-> "created"]
    /\ relation_nexus_operation_workflow = {}
    /\ relation_nexus_timeout_evidence = {}

NexusOperation_backing_off_schedule_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_schedule_Embedded(op) ==
    /\ NexusOperation_backing_off_schedule_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_backing_off_schedule_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_schedule_Standalone(op) ==
    /\ NexusOperation_backing_off_schedule_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_backing_off_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "backing_off"

NexusOperation_backing_off_terminate_Embedded(entity) ==
    /\ NexusOperation_backing_off_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_backing_off_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_terminate_Standalone(op) ==
    /\ NexusOperation_backing_off_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_attempt_failed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_attempt_failed_Embedded(op) ==
    /\ NexusOperation_scheduled_attempt_failed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "backing_off"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_attempt_failed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_attempt_failed_Standalone(op) ==
    /\ NexusOperation_scheduled_attempt_failed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "backing_off"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_cancel_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_cancel_Embedded(op) ==
    /\ NexusOperation_scheduled_cancel_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_cancel_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_cancel_Standalone(op) ==
    /\ NexusOperation_scheduled_cancel_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_fail_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_fail_Embedded(op) ==
    /\ NexusOperation_scheduled_fail_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_fail_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_fail_Standalone(op) ==
    /\ NexusOperation_scheduled_fail_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_start_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_start_Embedded(op) ==
    /\ NexusOperation_scheduled_start_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "started"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_start_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_start_Standalone(op) ==
    /\ NexusOperation_scheduled_start_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "started"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_succeed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_succeed_Embedded(op) ==
    /\ NexusOperation_scheduled_succeed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_succeed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_succeed_Standalone(op) ==
    /\ NexusOperation_scheduled_succeed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "scheduled"

NexusOperation_scheduled_terminate_Embedded(entity) ==
    /\ NexusOperation_scheduled_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_scheduled_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_terminate_Standalone(op) ==
    /\ NexusOperation_scheduled_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_started_cancel_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_cancel_Embedded(op) ==
    /\ NexusOperation_started_cancel_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_started_cancel_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_cancel_Standalone(op) ==
    /\ NexusOperation_started_cancel_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_started_fail_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_fail_Embedded(op) ==
    /\ NexusOperation_started_fail_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_started_fail_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_fail_Standalone(op) ==
    /\ NexusOperation_started_fail_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_started_succeed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_succeed_Embedded(op) ==
    /\ NexusOperation_started_succeed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_started_succeed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_succeed_Standalone(op) ==
    /\ NexusOperation_started_succeed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_started_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_terminate_Embedded(entity) ==
    /\ NexusOperation_started_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_started_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_terminate_Standalone(op) ==
    /\ NexusOperation_started_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_unspecified_reject_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \notin exists_NexusOperation
    /\ state_NexusOperation[entity] = "unspecified"

NexusOperation_unspecified_reject_Embedded(entity) ==
    /\ NexusOperation_unspecified_reject_EmbeddedEnabled(entity)
    /\ exists_NexusOperation' = exists_NexusOperation \union {entity}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "rejected"]
    /\ UNCHANGED <<exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

NexusOperation_unspecified_reject_StandaloneEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \notin exists_NexusOperation
    /\ state_NexusOperation[entity] = "unspecified"

NexusOperation_unspecified_reject_Standalone(entity) ==
    /\ NexusOperation_unspecified_reject_StandaloneEnabled(entity)
    /\ exists_NexusOperation' = exists_NexusOperation \union {entity}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "rejected"]
    /\ UNCHANGED <<exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

Nexus_operation_schedule_EmbeddedEnabled(op, workflow) ==
    /\ op \in NexusOperationIDs
    /\ op \notin exists_NexusOperation
    /\ workflow \in WorkflowIDs
    /\ workflow \in exists_Workflow
    /\ (state_Workflow[workflow] = "started" /\ state_NexusOperation[op] = "unspecified")

Nexus_operation_schedule_Embedded(op, workflow) ==
    /\ Nexus_operation_schedule_EmbeddedEnabled(op, workflow)
    /\ exists_NexusOperation' = exists_NexusOperation \union {op}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ relation_nexus_operation_workflow' = (relation_nexus_operation_workflow) \union {<<op, workflow>>}
    /\ UNCHANGED <<exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_timeout_evidence>>

Nexus_operation_schedule_StandaloneEnabled(op, workflow) ==
    /\ op \in NexusOperationIDs
    /\ op \notin exists_NexusOperation
    /\ workflow \in WorkflowIDs
    /\ workflow \in exists_Workflow
    /\ (state_Workflow[workflow] = "started" /\ state_NexusOperation[op] = "unspecified")

Nexus_operation_schedule_Standalone(op, workflow) ==
    /\ Nexus_operation_schedule_StandaloneEnabled(op, workflow)
    /\ exists_NexusOperation' = exists_NexusOperation \union {op}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ relation_nexus_operation_workflow' = (relation_nexus_operation_workflow) \union {<<op, workflow>>}
    /\ UNCHANGED <<exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, state_Workflow, relation_nexus_timeout_evidence>>

Nexus_timeout_backing_off_EmbeddedEnabled(op, timeoutEvidence) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ timeoutEvidence \in NexusTimeoutEvidenceIDs
    /\ timeoutEvidence \notin exists_NexusTimeoutEvidence
    /\ state_NexusOperation[op] = "backing_off"

Nexus_timeout_backing_off_Embedded(op, timeoutEvidence) ==
    /\ Nexus_timeout_backing_off_EmbeddedEnabled(op, timeoutEvidence)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ exists_NexusTimeoutEvidence' = exists_NexusTimeoutEvidence \union {timeoutEvidence}
    /\ state_NexusTimeoutEvidence' = [state_NexusTimeoutEvidence EXCEPT ![timeoutEvidence] = "valid"]
    /\ relation_nexus_timeout_evidence' = (relation_nexus_timeout_evidence) \union {<<op, timeoutEvidence>>}
    /\ UNCHANGED <<exists_NexusOperation, exists_Workflow, state_Workflow, relation_nexus_operation_workflow>>

Nexus_timeout_backing_off_StandaloneEnabled(op, timeoutEvidence) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ timeoutEvidence \in NexusTimeoutEvidenceIDs
    /\ timeoutEvidence \notin exists_NexusTimeoutEvidence
    /\ state_NexusOperation[op] = "backing_off"

Nexus_timeout_backing_off_Standalone(op, timeoutEvidence) ==
    /\ Nexus_timeout_backing_off_StandaloneEnabled(op, timeoutEvidence)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ exists_NexusTimeoutEvidence' = exists_NexusTimeoutEvidence \union {timeoutEvidence}
    /\ state_NexusTimeoutEvidence' = [state_NexusTimeoutEvidence EXCEPT ![timeoutEvidence] = "valid"]
    /\ relation_nexus_timeout_evidence' = (relation_nexus_timeout_evidence) \union {<<op, timeoutEvidence>>}
    /\ UNCHANGED <<exists_NexusOperation, exists_Workflow, state_Workflow, relation_nexus_operation_workflow>>

Nexus_timeout_scheduled_EmbeddedEnabled(op, timeoutEvidence) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ timeoutEvidence \in NexusTimeoutEvidenceIDs
    /\ timeoutEvidence \notin exists_NexusTimeoutEvidence
    /\ state_NexusOperation[op] = "scheduled"

Nexus_timeout_scheduled_Embedded(op, timeoutEvidence) ==
    /\ Nexus_timeout_scheduled_EmbeddedEnabled(op, timeoutEvidence)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ exists_NexusTimeoutEvidence' = exists_NexusTimeoutEvidence \union {timeoutEvidence}
    /\ state_NexusTimeoutEvidence' = [state_NexusTimeoutEvidence EXCEPT ![timeoutEvidence] = "valid"]
    /\ relation_nexus_timeout_evidence' = (relation_nexus_timeout_evidence) \union {<<op, timeoutEvidence>>}
    /\ UNCHANGED <<exists_NexusOperation, exists_Workflow, state_Workflow, relation_nexus_operation_workflow>>

Nexus_timeout_scheduled_StandaloneEnabled(op, timeoutEvidence) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ timeoutEvidence \in NexusTimeoutEvidenceIDs
    /\ timeoutEvidence \notin exists_NexusTimeoutEvidence
    /\ state_NexusOperation[op] = "scheduled"

Nexus_timeout_scheduled_Standalone(op, timeoutEvidence) ==
    /\ Nexus_timeout_scheduled_StandaloneEnabled(op, timeoutEvidence)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ exists_NexusTimeoutEvidence' = exists_NexusTimeoutEvidence \union {timeoutEvidence}
    /\ state_NexusTimeoutEvidence' = [state_NexusTimeoutEvidence EXCEPT ![timeoutEvidence] = "valid"]
    /\ relation_nexus_timeout_evidence' = (relation_nexus_timeout_evidence) \union {<<op, timeoutEvidence>>}
    /\ UNCHANGED <<exists_NexusOperation, exists_Workflow, state_Workflow, relation_nexus_operation_workflow>>

Nexus_timeout_started_EmbeddedEnabled(entity, timeoutEvidence) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ timeoutEvidence \in NexusTimeoutEvidenceIDs
    /\ timeoutEvidence \notin exists_NexusTimeoutEvidence
    /\ state_NexusOperation[entity] = "started"

Nexus_timeout_started_Embedded(entity, timeoutEvidence) ==
    /\ Nexus_timeout_started_EmbeddedEnabled(entity, timeoutEvidence)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "timed_out"]
    /\ exists_NexusTimeoutEvidence' = exists_NexusTimeoutEvidence \union {timeoutEvidence}
    /\ state_NexusTimeoutEvidence' = [state_NexusTimeoutEvidence EXCEPT ![timeoutEvidence] = "valid"]
    /\ relation_nexus_timeout_evidence' = (relation_nexus_timeout_evidence) \union {<<entity, timeoutEvidence>>}
    /\ UNCHANGED <<exists_NexusOperation, exists_Workflow, state_Workflow, relation_nexus_operation_workflow>>

Nexus_timeout_started_StandaloneEnabled(entity, timeoutEvidence) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ timeoutEvidence \in NexusTimeoutEvidenceIDs
    /\ timeoutEvidence \notin exists_NexusTimeoutEvidence
    /\ state_NexusOperation[entity] = "started"

Nexus_timeout_started_Standalone(entity, timeoutEvidence) ==
    /\ Nexus_timeout_started_StandaloneEnabled(entity, timeoutEvidence)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "timed_out"]
    /\ exists_NexusTimeoutEvidence' = exists_NexusTimeoutEvidence \union {timeoutEvidence}
    /\ state_NexusTimeoutEvidence' = [state_NexusTimeoutEvidence EXCEPT ![timeoutEvidence] = "valid"]
    /\ relation_nexus_timeout_evidence' = (relation_nexus_timeout_evidence) \union {<<entity, timeoutEvidence>>}
    /\ UNCHANGED <<exists_NexusOperation, exists_Workflow, state_Workflow, relation_nexus_operation_workflow>>

Nexus_workflow_close_cancelEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ (state_Workflow[entity] = "started" /\ (\A operation \in NexusOperationIDs: operation \in exists_NexusOperation => ((<<operation, entity>> \in relation_nexus_operation_workflow => (state_NexusOperation[operation] = "succeeded" \/ state_NexusOperation[operation] = "failed" \/ state_NexusOperation[operation] = "canceled" \/ state_NexusOperation[operation] = "timed_out" \/ state_NexusOperation[operation] = "terminated" \/ state_NexusOperation[operation] = "rejected")))))

Nexus_workflow_close_cancel(entity) ==
    /\ Nexus_workflow_close_cancelEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_NexusOperation, state_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

Nexus_workflow_close_completeEnabled(wf) ==
    /\ wf \in WorkflowIDs
    /\ wf \in exists_Workflow
    /\ (state_Workflow[wf] = "started" /\ (\A operation \in NexusOperationIDs: operation \in exists_NexusOperation => ((<<operation, wf>> \in relation_nexus_operation_workflow => (state_NexusOperation[operation] = "succeeded" \/ state_NexusOperation[operation] = "failed" \/ state_NexusOperation[operation] = "canceled" \/ state_NexusOperation[operation] = "timed_out" \/ state_NexusOperation[operation] = "terminated" \/ state_NexusOperation[operation] = "rejected")))))

Nexus_workflow_close_complete(wf) ==
    /\ Nexus_workflow_close_completeEnabled(wf)
    /\ state_Workflow' = [state_Workflow EXCEPT ![wf] = "completed"]
    /\ UNCHANGED <<exists_NexusOperation, state_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

Nexus_workflow_close_failEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ (state_Workflow[entity] = "started" /\ (\A operation \in NexusOperationIDs: operation \in exists_NexusOperation => ((<<operation, entity>> \in relation_nexus_operation_workflow => (state_NexusOperation[operation] = "succeeded" \/ state_NexusOperation[operation] = "failed" \/ state_NexusOperation[operation] = "canceled" \/ state_NexusOperation[operation] = "timed_out" \/ state_NexusOperation[operation] = "terminated" \/ state_NexusOperation[operation] = "rejected")))))

Nexus_workflow_close_fail(entity) ==
    /\ Nexus_workflow_close_failEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_NexusOperation, state_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

Nexus_workflow_close_terminateEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ (state_Workflow[entity] = "started" /\ (\A operation \in NexusOperationIDs: operation \in exists_NexusOperation => ((<<operation, entity>> \in relation_nexus_operation_workflow => (state_NexusOperation[operation] = "succeeded" \/ state_NexusOperation[operation] = "failed" \/ state_NexusOperation[operation] = "canceled" \/ state_NexusOperation[operation] = "timed_out" \/ state_NexusOperation[operation] = "terminated" \/ state_NexusOperation[operation] = "rejected")))))

Nexus_workflow_close_terminate(entity) ==
    /\ Nexus_workflow_close_terminateEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_NexusOperation, state_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

Nexus_workflow_close_timeoutEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ (state_Workflow[entity] = "started" /\ (\A operation \in NexusOperationIDs: operation \in exists_NexusOperation => ((<<operation, entity>> \in relation_nexus_operation_workflow => (state_NexusOperation[operation] = "succeeded" \/ state_NexusOperation[operation] = "failed" \/ state_NexusOperation[operation] = "canceled" \/ state_NexusOperation[operation] = "timed_out" \/ state_NexusOperation[operation] = "terminated" \/ state_NexusOperation[operation] = "rejected")))))

Nexus_workflow_close_timeout(entity) ==
    /\ Nexus_workflow_close_timeoutEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_NexusOperation, state_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, exists_Workflow, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

Nexus_workflow_startEnabled(wf) ==
    /\ wf \in WorkflowIDs
    /\ wf \notin exists_Workflow
    /\ state_Workflow[wf] = "created"

Nexus_workflow_start(wf) ==
    /\ Nexus_workflow_startEnabled(wf)
    /\ exists_Workflow' = exists_Workflow \union {wf}
    /\ state_Workflow' = [state_Workflow EXCEPT ![wf] = "started"]
    /\ UNCHANGED <<exists_NexusOperation, state_NexusOperation, exists_NexusTimeoutEvidence, state_NexusTimeoutEvidence, relation_nexus_operation_workflow, relation_nexus_timeout_evidence>>

Next ==
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_schedule_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_schedule_Standalone(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_backing_off_terminate_Embedded(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_terminate_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_attempt_failed_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_attempt_failed_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_cancel_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_cancel_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_fail_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_fail_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_start_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_start_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_succeed_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_succeed_Standalone(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_scheduled_terminate_Embedded(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_terminate_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_cancel_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_cancel_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_fail_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_fail_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_succeed_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_succeed_Standalone(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_started_terminate_Embedded(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_terminate_Standalone(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_unspecified_reject_Embedded(entity)
    \/ \E entity \in NexusOperationIDs: NexusOperation_unspecified_reject_Standalone(entity)
    \/ \E op \in NexusOperationIDs, workflow \in WorkflowIDs: Nexus_operation_schedule_Embedded(op, workflow)
    \/ \E op \in NexusOperationIDs, workflow \in WorkflowIDs: Nexus_operation_schedule_Standalone(op, workflow)
    \/ \E op \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_backing_off_Embedded(op, timeoutEvidence)
    \/ \E op \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_backing_off_Standalone(op, timeoutEvidence)
    \/ \E op \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_scheduled_Embedded(op, timeoutEvidence)
    \/ \E op \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_scheduled_Standalone(op, timeoutEvidence)
    \/ \E entity \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_started_Embedded(entity, timeoutEvidence)
    \/ \E entity \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_started_Standalone(entity, timeoutEvidence)
    \/ \E entity \in WorkflowIDs: Nexus_workflow_close_cancel(entity)
    \/ \E wf \in WorkflowIDs: Nexus_workflow_close_complete(wf)
    \/ \E entity \in WorkflowIDs: Nexus_workflow_close_fail(entity)
    \/ \E entity \in WorkflowIDs: Nexus_workflow_close_terminate(entity)
    \/ \E entity \in WorkflowIDs: Nexus_workflow_close_timeout(entity)
    \/ \E wf \in WorkflowIDs: Nexus_workflow_start(wf)

CanStep ==
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_schedule_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_schedule_StandaloneEnabled(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_backing_off_terminate_EmbeddedEnabled(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_terminate_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_attempt_failed_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_attempt_failed_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_cancel_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_cancel_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_fail_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_fail_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_start_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_start_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_succeed_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_succeed_StandaloneEnabled(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_scheduled_terminate_EmbeddedEnabled(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_terminate_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_cancel_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_cancel_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_fail_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_fail_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_succeed_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_succeed_StandaloneEnabled(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_started_terminate_EmbeddedEnabled(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_terminate_StandaloneEnabled(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_unspecified_reject_EmbeddedEnabled(entity)
    \/ \E entity \in NexusOperationIDs: NexusOperation_unspecified_reject_StandaloneEnabled(entity)
    \/ \E op \in NexusOperationIDs, workflow \in WorkflowIDs: Nexus_operation_schedule_EmbeddedEnabled(op, workflow)
    \/ \E op \in NexusOperationIDs, workflow \in WorkflowIDs: Nexus_operation_schedule_StandaloneEnabled(op, workflow)
    \/ \E op \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_backing_off_EmbeddedEnabled(op, timeoutEvidence)
    \/ \E op \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_backing_off_StandaloneEnabled(op, timeoutEvidence)
    \/ \E op \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_scheduled_EmbeddedEnabled(op, timeoutEvidence)
    \/ \E op \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_scheduled_StandaloneEnabled(op, timeoutEvidence)
    \/ \E entity \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_started_EmbeddedEnabled(entity, timeoutEvidence)
    \/ \E entity \in NexusOperationIDs, timeoutEvidence \in NexusTimeoutEvidenceIDs: Nexus_timeout_started_StandaloneEnabled(entity, timeoutEvidence)
    \/ \E entity \in WorkflowIDs: Nexus_workflow_close_cancelEnabled(entity)
    \/ \E wf \in WorkflowIDs: Nexus_workflow_close_completeEnabled(wf)
    \/ \E entity \in WorkflowIDs: Nexus_workflow_close_failEnabled(entity)
    \/ \E entity \in WorkflowIDs: Nexus_workflow_close_terminateEnabled(entity)
    \/ \E entity \in WorkflowIDs: Nexus_workflow_close_timeoutEnabled(entity)
    \/ \E wf \in WorkflowIDs: Nexus_workflow_startEnabled(wf)

NexusOperation_backing_off_quiescent_progress ==
    (\A entity \in NexusOperationIDs: entity \in exists_NexusOperation => (~(state_NexusOperation[entity] = "backing_off")))

NexusOperation_scheduled_quiescent_progress ==
    (\A entity \in NexusOperationIDs: entity \in exists_NexusOperation => (~(state_NexusOperation[entity] = "scheduled")))

NexusOperation_started_quiescent_progress ==
    (\A entity \in NexusOperationIDs: entity \in exists_NexusOperation => (~(state_NexusOperation[entity] = "started")))

NexusOperationClosure ==
    (\A operation \in NexusOperationIDs: operation \in exists_NexusOperation => ((\A workflow \in WorkflowIDs: workflow \in exists_Workflow => (((<<operation, workflow>> \in relation_nexus_operation_workflow /\ (state_Workflow[workflow] = "completed" \/ state_Workflow[workflow] = "failed" \/ state_Workflow[workflow] = "canceled" \/ state_Workflow[workflow] = "terminated" \/ state_Workflow[workflow] = "timed_out")) => (state_NexusOperation[operation] = "succeeded" \/ state_NexusOperation[operation] = "failed" \/ state_NexusOperation[operation] = "canceled" \/ state_NexusOperation[operation] = "timed_out" \/ state_NexusOperation[operation] = "terminated" \/ state_NexusOperation[operation] = "rejected"))))))

NexusOperationTimeoutSemantics ==
    (\A operation \in NexusOperationIDs: operation \in exists_NexusOperation => ((state_NexusOperation[operation] = "timed_out" => (\E timeoutEvidence \in NexusTimeoutEvidenceIDs: timeoutEvidence \in exists_NexusTimeoutEvidence /\ ((<<operation, timeoutEvidence>> \in relation_nexus_timeout_evidence /\ state_NexusTimeoutEvidence[timeoutEvidence] = "valid"))))))

InductiveInvariant ==
    /\ TypeOK
    /\ Cardinality_nexus_operation_workflow
    /\ Cardinality_nexus_timeout_evidence
    /\ NexusOperationClosure
    /\ NexusOperationTimeoutSemantics
DeclaredSafety ==
    /\ NexusOperationTimeoutSemantics
Safety == InductiveInvariant /\ DeclaredSafety

QuiescentSafety ==
    \/ CanStep
    \/ /\ NexusOperation_backing_off_quiescent_progress
       /\ NexusOperation_scheduled_quiescent_progress
       /\ NexusOperation_started_quiescent_progress

Spec == Init /\ [][Next]_vars

====
