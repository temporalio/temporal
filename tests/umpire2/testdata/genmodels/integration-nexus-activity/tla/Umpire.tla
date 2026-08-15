---- MODULE Umpire ----
EXTENDS FiniteSets, Naturals

CONSTANTS
    \* @type: Set(Str);
    ActivityIDs,
    \* @type: Set(Str);
    NexusOperationIDs

VARIABLES
    \* @type: Set(Str);
    exists_Activity,
    \* @type: Str -> Str;
    state_Activity,
    \* @type: Set(Str);
    exists_NexusOperation,
    \* @type: Str -> Str;
    state_NexusOperation,
    \* @type: Set(<<Str, Str>>);
    relation_activity_nexus,
    \* @type: Set(<<Str, Str>>);
    relation_nexus_activity

vars == <<exists_Activity, state_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

TypeOK ==
    /\ exists_Activity \in SUBSET ActivityIDs
    /\ state_Activity \in [ActivityIDs -> {"backing_off", "canceled", "completed", "failed", "scheduled", "started", "timed_out", "unspecified"}]
    /\ exists_NexusOperation \in SUBSET NexusOperationIDs
    /\ state_NexusOperation \in [NexusOperationIDs -> {"backing_off", "canceled", "failed", "rejected", "scheduled", "started", "succeeded", "terminated", "timed_out", "unspecified"}]
    /\ relation_activity_nexus \in SUBSET (ActivityIDs \X NexusOperationIDs)
    /\ relation_nexus_activity \in SUBSET (NexusOperationIDs \X ActivityIDs)

Cardinality_activity_nexus ==
    /\ \A tuple \in relation_activity_nexus: tuple[1] \in exists_Activity /\ tuple[2] \in exists_NexusOperation
    /\ \A source \in ActivityIDs: Cardinality({target \in NexusOperationIDs: <<source, target>> \in relation_activity_nexus}) <= 1
    /\ \A target \in NexusOperationIDs: Cardinality({source \in ActivityIDs: <<source, target>> \in relation_activity_nexus}) <= 1

Cardinality_nexus_activity ==
    /\ \A tuple \in relation_nexus_activity: tuple[1] \in exists_NexusOperation /\ tuple[2] \in exists_Activity
    /\ \A source \in NexusOperationIDs: Cardinality({target \in ActivityIDs: <<source, target>> \in relation_nexus_activity}) <= 1
    /\ \A target \in ActivityIDs: Cardinality({source \in NexusOperationIDs: <<source, target>> \in relation_nexus_activity}) <= 1

Init ==
    /\ exists_Activity = {}
    /\ state_Activity = [entity \in ActivityIDs |-> "unspecified"]
    /\ exists_NexusOperation = {}
    /\ state_NexusOperation = [entity \in NexusOperationIDs |-> "unspecified"]
    /\ relation_activity_nexus = {}
    /\ relation_nexus_activity = {}

Activity_backing_off_cancel_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "backing_off"

Activity_backing_off_cancel_AnyHosting(entity) ==
    /\ Activity_backing_off_cancel_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_backing_off_schedule_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "backing_off"

Activity_backing_off_schedule_AnyHosting(entity) ==
    /\ Activity_backing_off_schedule_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_backing_off_timeout_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "backing_off"

Activity_backing_off_timeout_AnyHosting(entity) ==
    /\ Activity_backing_off_timeout_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_scheduled_cancel_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "scheduled"

Activity_scheduled_cancel_AnyHosting(entity) ==
    /\ Activity_scheduled_cancel_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_scheduled_fail_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "scheduled"

Activity_scheduled_fail_AnyHosting(entity) ==
    /\ Activity_scheduled_fail_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_scheduled_start_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "scheduled"

Activity_scheduled_start_AnyHosting(entity) ==
    /\ Activity_scheduled_start_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "started"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_scheduled_timeout_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "scheduled"

Activity_scheduled_timeout_AnyHosting(entity) ==
    /\ Activity_scheduled_timeout_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_started_attempt_failed_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_attempt_failed_AnyHosting(entity) ==
    /\ Activity_started_attempt_failed_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "backing_off"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_started_cancel_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_cancel_AnyHosting(entity) ==
    /\ Activity_started_cancel_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_started_complete_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_complete_AnyHosting(entity) ==
    /\ Activity_started_complete_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "completed"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_started_fail_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_fail_AnyHosting(entity) ==
    /\ Activity_started_fail_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_started_timeout_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_timeout_AnyHosting(entity) ==
    /\ Activity_started_timeout_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

Activity_unspecified_schedule_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \notin exists_Activity
    /\ state_Activity[entity] = "unspecified"

Activity_unspecified_schedule_AnyHosting(entity) ==
    /\ Activity_unspecified_schedule_AnyHostingEnabled(entity)
    /\ exists_Activity' = exists_Activity \union {entity}
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "scheduled"]
    /\ UNCHANGED <<exists_NexusOperation, state_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_backing_off_schedule_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_schedule_Embedded(op) ==
    /\ NexusOperation_backing_off_schedule_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_backing_off_schedule_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_schedule_Standalone(op) ==
    /\ NexusOperation_backing_off_schedule_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_backing_off_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "backing_off"

NexusOperation_backing_off_terminate_Embedded(entity) ==
    /\ NexusOperation_backing_off_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_backing_off_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_terminate_Standalone(op) ==
    /\ NexusOperation_backing_off_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_backing_off_timeout_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_timeout_Embedded(op) ==
    /\ NexusOperation_backing_off_timeout_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_backing_off_timeout_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_timeout_Standalone(op) ==
    /\ NexusOperation_backing_off_timeout_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_attempt_failed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_attempt_failed_Embedded(op) ==
    /\ NexusOperation_scheduled_attempt_failed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "backing_off"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_attempt_failed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_attempt_failed_Standalone(op) ==
    /\ NexusOperation_scheduled_attempt_failed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "backing_off"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_cancel_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_cancel_Embedded(op) ==
    /\ NexusOperation_scheduled_cancel_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_cancel_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_cancel_Standalone(op) ==
    /\ NexusOperation_scheduled_cancel_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_fail_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_fail_Embedded(op) ==
    /\ NexusOperation_scheduled_fail_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_fail_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_fail_Standalone(op) ==
    /\ NexusOperation_scheduled_fail_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_start_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_start_Embedded(op) ==
    /\ NexusOperation_scheduled_start_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "started"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_start_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_start_Standalone(op) ==
    /\ NexusOperation_scheduled_start_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "started"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_succeed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_succeed_Embedded(op) ==
    /\ NexusOperation_scheduled_succeed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_succeed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_succeed_Standalone(op) ==
    /\ NexusOperation_scheduled_succeed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "scheduled"

NexusOperation_scheduled_terminate_Embedded(entity) ==
    /\ NexusOperation_scheduled_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_terminate_Standalone(op) ==
    /\ NexusOperation_scheduled_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_timeout_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_timeout_Embedded(op) ==
    /\ NexusOperation_scheduled_timeout_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_scheduled_timeout_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_timeout_Standalone(op) ==
    /\ NexusOperation_scheduled_timeout_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_cancel_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_cancel_Embedded(op) ==
    /\ NexusOperation_started_cancel_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_cancel_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_cancel_Standalone(op) ==
    /\ NexusOperation_started_cancel_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_fail_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_fail_Embedded(op) ==
    /\ NexusOperation_started_fail_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_fail_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_fail_Standalone(op) ==
    /\ NexusOperation_started_fail_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_succeed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_succeed_Embedded(op) ==
    /\ NexusOperation_started_succeed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_succeed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_succeed_Standalone(op) ==
    /\ NexusOperation_started_succeed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_terminate_Embedded(entity) ==
    /\ NexusOperation_started_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_terminate_Standalone(op) ==
    /\ NexusOperation_started_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_timeout_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_timeout_Embedded(entity) ==
    /\ NexusOperation_started_timeout_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_started_timeout_StandaloneEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_timeout_Standalone(entity) ==
    /\ NexusOperation_started_timeout_StandaloneEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_NexusOperation, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_unspecified_reject_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \notin exists_NexusOperation
    /\ state_NexusOperation[entity] = "unspecified"

NexusOperation_unspecified_reject_Embedded(entity) ==
    /\ NexusOperation_unspecified_reject_EmbeddedEnabled(entity)
    /\ exists_NexusOperation' = exists_NexusOperation \union {entity}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "rejected"]
    /\ UNCHANGED <<exists_Activity, state_Activity, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_unspecified_reject_StandaloneEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \notin exists_NexusOperation
    /\ state_NexusOperation[entity] = "unspecified"

NexusOperation_unspecified_reject_Standalone(entity) ==
    /\ NexusOperation_unspecified_reject_StandaloneEnabled(entity)
    /\ exists_NexusOperation' = exists_NexusOperation \union {entity}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "rejected"]
    /\ UNCHANGED <<exists_Activity, state_Activity, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_unspecified_schedule_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \notin exists_NexusOperation
    /\ state_NexusOperation[op] = "unspecified"

NexusOperation_unspecified_schedule_Embedded(op) ==
    /\ NexusOperation_unspecified_schedule_EmbeddedEnabled(op)
    /\ exists_NexusOperation' = exists_NexusOperation \union {op}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, relation_activity_nexus, relation_nexus_activity>>

NexusOperation_unspecified_schedule_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \notin exists_NexusOperation
    /\ state_NexusOperation[op] = "unspecified"

NexusOperation_unspecified_schedule_Standalone(op) ==
    /\ NexusOperation_unspecified_schedule_StandaloneEnabled(op)
    /\ exists_NexusOperation' = exists_NexusOperation \union {op}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, relation_activity_nexus, relation_nexus_activity>>

Regression_nexus_start_activityEnabled(activity, operation) ==
    /\ activity \in ActivityIDs
    /\ activity \notin exists_Activity
    /\ operation \in NexusOperationIDs
    /\ operation \in exists_NexusOperation
    /\ state_NexusOperation[operation] = "scheduled"

Regression_nexus_start_activity(activity, operation) ==
    /\ Regression_nexus_start_activityEnabled(activity, operation)
    /\ exists_Activity' = exists_Activity \union {activity}
    /\ state_Activity' = [state_Activity EXCEPT ![activity] = "completed"]
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![operation] = "succeeded"]
    /\ relation_activity_nexus' = (relation_activity_nexus) \union {<<activity, operation>>}
    /\ relation_nexus_activity' = (relation_nexus_activity) \union {<<operation, activity>>}
    /\ UNCHANGED <<exists_NexusOperation>>

Next ==
    \/ \E entity \in ActivityIDs: Activity_backing_off_cancel_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_backing_off_schedule_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_backing_off_timeout_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_scheduled_cancel_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_scheduled_fail_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_scheduled_start_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_scheduled_timeout_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_started_attempt_failed_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_started_cancel_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_started_complete_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_started_fail_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_started_timeout_AnyHosting(entity)
    \/ \E entity \in ActivityIDs: Activity_unspecified_schedule_AnyHosting(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_schedule_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_schedule_Standalone(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_backing_off_terminate_Embedded(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_terminate_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_timeout_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_timeout_Standalone(op)
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
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_timeout_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_timeout_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_cancel_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_cancel_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_fail_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_fail_Standalone(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_succeed_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_succeed_Standalone(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_started_terminate_Embedded(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_terminate_Standalone(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_started_timeout_Embedded(entity)
    \/ \E entity \in NexusOperationIDs: NexusOperation_started_timeout_Standalone(entity)
    \/ \E entity \in NexusOperationIDs: NexusOperation_unspecified_reject_Embedded(entity)
    \/ \E entity \in NexusOperationIDs: NexusOperation_unspecified_reject_Standalone(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_unspecified_schedule_Embedded(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_unspecified_schedule_Standalone(op)
    \/ \E activity \in ActivityIDs, operation \in NexusOperationIDs: Regression_nexus_start_activity(activity, operation)

CanStep ==
    \/ \E entity \in ActivityIDs: Activity_backing_off_cancel_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_backing_off_schedule_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_backing_off_timeout_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_scheduled_cancel_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_scheduled_fail_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_scheduled_start_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_scheduled_timeout_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_started_attempt_failed_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_started_cancel_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_started_complete_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_started_fail_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_started_timeout_AnyHostingEnabled(entity)
    \/ \E entity \in ActivityIDs: Activity_unspecified_schedule_AnyHostingEnabled(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_schedule_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_schedule_StandaloneEnabled(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_backing_off_terminate_EmbeddedEnabled(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_terminate_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_timeout_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_backing_off_timeout_StandaloneEnabled(op)
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
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_timeout_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_scheduled_timeout_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_cancel_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_cancel_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_fail_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_fail_StandaloneEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_succeed_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_succeed_StandaloneEnabled(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_started_terminate_EmbeddedEnabled(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_started_terminate_StandaloneEnabled(op)
    \/ \E entity \in NexusOperationIDs: NexusOperation_started_timeout_EmbeddedEnabled(entity)
    \/ \E entity \in NexusOperationIDs: NexusOperation_started_timeout_StandaloneEnabled(entity)
    \/ \E entity \in NexusOperationIDs: NexusOperation_unspecified_reject_EmbeddedEnabled(entity)
    \/ \E entity \in NexusOperationIDs: NexusOperation_unspecified_reject_StandaloneEnabled(entity)
    \/ \E op \in NexusOperationIDs: NexusOperation_unspecified_schedule_EmbeddedEnabled(op)
    \/ \E op \in NexusOperationIDs: NexusOperation_unspecified_schedule_StandaloneEnabled(op)
    \/ \E activity \in ActivityIDs, operation \in NexusOperationIDs: Regression_nexus_start_activityEnabled(activity, operation)

Activity_backing_off_quiescent_progress ==
    (\A entity \in ActivityIDs: entity \in exists_Activity => (~(state_Activity[entity] = "backing_off")))

Activity_scheduled_quiescent_progress ==
    (\A entity \in ActivityIDs: entity \in exists_Activity => (~(state_Activity[entity] = "scheduled")))

Activity_started_quiescent_progress ==
    (\A entity \in ActivityIDs: entity \in exists_Activity => (~(state_Activity[entity] = "started")))

NexusActivityForwardLinkConsistency ==
    (\A source \in NexusOperationIDs: source \in exists_NexusOperation => ((\A target \in ActivityIDs: target \in exists_Activity => ((<<source, target>> \in relation_nexus_activity => <<target, source>> \in relation_activity_nexus)))))

NexusActivityReverseLinkConsistency ==
    (\A source \in ActivityIDs: source \in exists_Activity => ((\A target \in NexusOperationIDs: target \in exists_NexusOperation => ((<<source, target>> \in relation_activity_nexus => <<target, source>> \in relation_nexus_activity)))))

NexusActivityTerminalRefinement ==
    (\A operation \in NexusOperationIDs: operation \in exists_NexusOperation => ((\A activity \in ActivityIDs: activity \in exists_Activity => ((<<operation, activity>> \in relation_nexus_activity => (state_NexusOperation[operation] = "succeeded" /\ state_Activity[activity] = "completed"))))))

NexusOperation_backing_off_quiescent_progress ==
    (\A entity \in NexusOperationIDs: entity \in exists_NexusOperation => (~(state_NexusOperation[entity] = "backing_off")))

NexusOperation_scheduled_quiescent_progress ==
    (\A entity \in NexusOperationIDs: entity \in exists_NexusOperation => (~(state_NexusOperation[entity] = "scheduled")))

NexusOperation_started_quiescent_progress ==
    (\A entity \in NexusOperationIDs: entity \in exists_NexusOperation => (~(state_NexusOperation[entity] = "started")))

InductiveInvariant ==
    /\ TypeOK
    /\ Cardinality_activity_nexus
    /\ Cardinality_nexus_activity
    /\ NexusActivityForwardLinkConsistency
    /\ NexusActivityReverseLinkConsistency
    /\ NexusActivityTerminalRefinement
DeclaredSafety ==
    /\ NexusActivityForwardLinkConsistency
    /\ NexusActivityReverseLinkConsistency
Safety == InductiveInvariant /\ DeclaredSafety

QuiescentSafety ==
    \/ CanStep
    \/ /\ Activity_backing_off_quiescent_progress
       /\ Activity_scheduled_quiescent_progress
       /\ Activity_started_quiescent_progress
       /\ NexusOperation_backing_off_quiescent_progress
       /\ NexusOperation_scheduled_quiescent_progress
       /\ NexusOperation_started_quiescent_progress

Spec == Init /\ [][Next]_vars

====
