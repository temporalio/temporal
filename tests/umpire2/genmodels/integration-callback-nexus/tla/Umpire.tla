---- MODULE Umpire ----
EXTENDS FiniteSets, Naturals

CONSTANTS
    \* @type: Set(Str);
    CallbackIDs,
    \* @type: Set(Str);
    CallbackDeliveryIDs,
    \* @type: Set(Str);
    CallbackResponseIDs,
    \* @type: Set(Str);
    NexusOperationIDs,
    \* @type: Set(Str);
    WorkflowRunIDs

VARIABLES
    \* @type: Set(Str);
    exists_Callback,
    \* @type: Str -> Str;
    state_Callback,
    \* @type: Set(Str);
    exists_CallbackDelivery,
    \* @type: Str -> Str;
    state_CallbackDelivery,
    \* @type: Set(Str);
    exists_CallbackResponse,
    \* @type: Str -> Str;
    state_CallbackResponse,
    \* @type: Set(Str);
    exists_NexusOperation,
    \* @type: Str -> Str;
    state_NexusOperation,
    \* @type: Set(Str);
    exists_WorkflowRun,
    \* @type: Str -> Str;
    state_WorkflowRun,
    \* @type: Set(<<Str, Str>>);
    relation_callback_delivery,
    \* @type: Set(<<Str, Str>>);
    relation_callback_delivery_response,
    \* @type: Set(<<Str, Str>>);
    relation_callback_handler_run,
    \* @type: Set(<<Str, Str>>);
    relation_callback_operation,
    \* @type: Set(<<Str, Str>>);
    relation_nexus_operation_handler_run

vars == <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

TypeOK ==
    /\ exists_Callback \in SUBSET CallbackIDs
    /\ state_Callback \in [CallbackIDs -> {"unobserved"}]
    /\ exists_CallbackDelivery \in SUBSET CallbackDeliveryIDs
    /\ state_CallbackDelivery \in [CallbackDeliveryIDs -> {"acknowledged", "delivered", "failed", "pending"}]
    /\ exists_CallbackResponse \in SUBSET CallbackResponseIDs
    /\ state_CallbackResponse \in [CallbackResponseIDs -> {"accepted", "conflicting", "unobserved"}]
    /\ exists_NexusOperation \in SUBSET NexusOperationIDs
    /\ state_NexusOperation \in [NexusOperationIDs -> {"backing_off", "canceled", "failed", "rejected", "scheduled", "started", "succeeded", "terminated", "timed_out", "unspecified"}]
    /\ exists_WorkflowRun \in SUBSET WorkflowRunIDs
    /\ state_WorkflowRun \in [WorkflowRunIDs -> {"canceled", "completed", "continued_as_new", "created", "failed", "started", "terminated", "timed_out"}]
    /\ relation_callback_delivery \in SUBSET (CallbackIDs \X CallbackDeliveryIDs)
    /\ relation_callback_delivery_response \in SUBSET (CallbackDeliveryIDs \X CallbackResponseIDs)
    /\ relation_callback_handler_run \in SUBSET (CallbackIDs \X WorkflowRunIDs)
    /\ relation_callback_operation \in SUBSET (CallbackIDs \X NexusOperationIDs)
    /\ relation_nexus_operation_handler_run \in SUBSET (NexusOperationIDs \X WorkflowRunIDs)

Cardinality_callback_delivery ==
    /\ \A tuple \in relation_callback_delivery: tuple[1] \in exists_Callback /\ tuple[2] \in exists_CallbackDelivery
    /\ \A target \in CallbackDeliveryIDs: Cardinality({source \in CallbackIDs: <<source, target>> \in relation_callback_delivery}) <= 1

Cardinality_callback_delivery_response ==
    /\ \A tuple \in relation_callback_delivery_response: tuple[1] \in exists_CallbackDelivery /\ tuple[2] \in exists_CallbackResponse
    /\ \A source \in CallbackDeliveryIDs: Cardinality({target \in CallbackResponseIDs: <<source, target>> \in relation_callback_delivery_response}) <= 1
    /\ \A target \in CallbackResponseIDs: Cardinality({source \in CallbackDeliveryIDs: <<source, target>> \in relation_callback_delivery_response}) <= 1

Cardinality_callback_handler_run ==
    /\ \A tuple \in relation_callback_handler_run: tuple[1] \in exists_Callback /\ tuple[2] \in exists_WorkflowRun
    /\ \A source \in CallbackIDs: Cardinality({target \in WorkflowRunIDs: <<source, target>> \in relation_callback_handler_run}) <= 1

Cardinality_callback_operation ==
    /\ \A tuple \in relation_callback_operation: tuple[1] \in exists_Callback /\ tuple[2] \in exists_NexusOperation
    /\ \A source \in CallbackIDs: Cardinality({target \in NexusOperationIDs: <<source, target>> \in relation_callback_operation}) <= 1

Cardinality_nexus_operation_handler_run ==
    /\ \A tuple \in relation_nexus_operation_handler_run: tuple[1] \in exists_NexusOperation /\ tuple[2] \in exists_WorkflowRun
    /\ \A source \in NexusOperationIDs: Cardinality({target \in WorkflowRunIDs: <<source, target>> \in relation_nexus_operation_handler_run}) <= 1

Init ==
    /\ exists_Callback = {}
    /\ state_Callback = [entity \in CallbackIDs |-> "unobserved"]
    /\ exists_CallbackDelivery = {}
    /\ state_CallbackDelivery = [entity \in CallbackDeliveryIDs |-> "pending"]
    /\ exists_CallbackResponse = {}
    /\ state_CallbackResponse = [entity \in CallbackResponseIDs |-> "unobserved"]
    /\ exists_NexusOperation = {}
    /\ state_NexusOperation = [entity \in NexusOperationIDs |-> "unspecified"]
    /\ exists_WorkflowRun = {}
    /\ state_WorkflowRun = [entity \in WorkflowRunIDs |-> "created"]
    /\ relation_callback_delivery = {}
    /\ relation_callback_delivery_response = {}
    /\ relation_callback_handler_run = {}
    /\ relation_callback_operation = {}
    /\ relation_nexus_operation_handler_run = {}

NexusOperation_backing_off_schedule_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_schedule_Embedded(op) ==
    /\ NexusOperation_backing_off_schedule_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_backing_off_schedule_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_schedule_Standalone(op) ==
    /\ NexusOperation_backing_off_schedule_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_backing_off_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "backing_off"

NexusOperation_backing_off_terminate_Embedded(entity) ==
    /\ NexusOperation_backing_off_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_backing_off_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_terminate_Standalone(op) ==
    /\ NexusOperation_backing_off_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_backing_off_timeout_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_timeout_Embedded(op) ==
    /\ NexusOperation_backing_off_timeout_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_backing_off_timeout_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_timeout_Standalone(op) ==
    /\ NexusOperation_backing_off_timeout_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_attempt_failed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_attempt_failed_Embedded(op) ==
    /\ NexusOperation_scheduled_attempt_failed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "backing_off"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_attempt_failed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_attempt_failed_Standalone(op) ==
    /\ NexusOperation_scheduled_attempt_failed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "backing_off"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_cancel_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_cancel_Embedded(op) ==
    /\ NexusOperation_scheduled_cancel_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_cancel_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_cancel_Standalone(op) ==
    /\ NexusOperation_scheduled_cancel_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_fail_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_fail_Embedded(op) ==
    /\ NexusOperation_scheduled_fail_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_fail_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_fail_Standalone(op) ==
    /\ NexusOperation_scheduled_fail_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_start_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_start_Embedded(op) ==
    /\ NexusOperation_scheduled_start_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "started"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_start_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_start_Standalone(op) ==
    /\ NexusOperation_scheduled_start_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "started"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_succeed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_succeed_Embedded(op) ==
    /\ NexusOperation_scheduled_succeed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_succeed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_succeed_Standalone(op) ==
    /\ NexusOperation_scheduled_succeed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "scheduled"

NexusOperation_scheduled_terminate_Embedded(entity) ==
    /\ NexusOperation_scheduled_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_terminate_Standalone(op) ==
    /\ NexusOperation_scheduled_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_timeout_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_timeout_Embedded(op) ==
    /\ NexusOperation_scheduled_timeout_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_scheduled_timeout_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_timeout_Standalone(op) ==
    /\ NexusOperation_scheduled_timeout_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_cancel_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_cancel_Embedded(op) ==
    /\ NexusOperation_started_cancel_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_cancel_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_cancel_Standalone(op) ==
    /\ NexusOperation_started_cancel_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_fail_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_fail_Embedded(op) ==
    /\ NexusOperation_started_fail_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_fail_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_fail_Standalone(op) ==
    /\ NexusOperation_started_fail_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_succeed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_succeed_Embedded(op) ==
    /\ NexusOperation_started_succeed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_succeed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_succeed_Standalone(op) ==
    /\ NexusOperation_started_succeed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_terminate_Embedded(entity) ==
    /\ NexusOperation_started_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_terminate_Standalone(op) ==
    /\ NexusOperation_started_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_timeout_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_timeout_Embedded(entity) ==
    /\ NexusOperation_started_timeout_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_started_timeout_StandaloneEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_timeout_Standalone(entity) ==
    /\ NexusOperation_started_timeout_StandaloneEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_unspecified_reject_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \notin exists_NexusOperation
    /\ state_NexusOperation[entity] = "unspecified"

NexusOperation_unspecified_reject_Embedded(entity) ==
    /\ NexusOperation_unspecified_reject_EmbeddedEnabled(entity)
    /\ exists_NexusOperation' = exists_NexusOperation \union {entity}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "rejected"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_unspecified_reject_StandaloneEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \notin exists_NexusOperation
    /\ state_NexusOperation[entity] = "unspecified"

NexusOperation_unspecified_reject_Standalone(entity) ==
    /\ NexusOperation_unspecified_reject_StandaloneEnabled(entity)
    /\ exists_NexusOperation' = exists_NexusOperation \union {entity}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "rejected"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_unspecified_schedule_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \notin exists_NexusOperation
    /\ state_NexusOperation[op] = "unspecified"

NexusOperation_unspecified_schedule_Embedded(op) ==
    /\ NexusOperation_unspecified_schedule_EmbeddedEnabled(op)
    /\ exists_NexusOperation' = exists_NexusOperation \union {op}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

NexusOperation_unspecified_schedule_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \notin exists_NexusOperation
    /\ state_NexusOperation[op] = "unspecified"

NexusOperation_unspecified_schedule_Standalone(op) ==
    /\ NexusOperation_unspecified_schedule_StandaloneEnabled(op)
    /\ exists_NexusOperation' = exists_NexusOperation \union {op}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_attach_handlerEnabled(callback, handlerRun) ==
    /\ callback \in CallbackIDs
    /\ callback \notin exists_Callback
    /\ handlerRun \in WorkflowRunIDs
    /\ handlerRun \in exists_WorkflowRun
    /\ state_WorkflowRun[handlerRun] = "started"

Callback_attach_handler(callback, handlerRun) ==
    /\ Callback_attach_handlerEnabled(callback, handlerRun)
    /\ exists_Callback' = exists_Callback \union {callback}
    /\ state_Callback' = [state_Callback EXCEPT ![callback] = "unobserved"]
    /\ relation_callback_handler_run' = relation_callback_handler_run \union {<<callback, handlerRun>>}
    /\ UNCHANGED <<exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_attach_referenceEnabled(callback, operation, handlerRun) ==
    /\ callback \in CallbackIDs
    /\ callback \notin exists_Callback
    /\ operation \in NexusOperationIDs
    /\ operation \in exists_NexusOperation
    /\ handlerRun \in WorkflowRunIDs
    /\ handlerRun \in exists_WorkflowRun
    /\ state_WorkflowRun[handlerRun] = "started"

Callback_attach_reference(callback, operation, handlerRun) ==
    /\ Callback_attach_referenceEnabled(callback, operation, handlerRun)
    /\ exists_Callback' = exists_Callback \union {callback}
    /\ state_Callback' = [state_Callback EXCEPT ![callback] = "unobserved"]
    /\ relation_callback_handler_run' = relation_callback_handler_run \union {<<callback, handlerRun>>}
    /\ relation_callback_operation' = relation_callback_operation \union {<<callback, operation>>}
    /\ relation_nexus_operation_handler_run' = relation_nexus_operation_handler_run \union {<<operation, handlerRun>>}
    /\ UNCHANGED <<exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response>>

Callback_delivery_acknowledgeEnabled(delivery, response) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ response \in CallbackResponseIDs
    /\ response \notin exists_CallbackResponse
    /\ state_CallbackDelivery[delivery] = "delivered"

Callback_delivery_acknowledge(delivery, response) ==
    /\ Callback_delivery_acknowledgeEnabled(delivery, response)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "acknowledged"]
    /\ exists_CallbackResponse' = exists_CallbackResponse \union {response}
    /\ state_CallbackResponse' = [state_CallbackResponse EXCEPT ![response] = "accepted"]
    /\ relation_callback_delivery_response' = relation_callback_delivery_response \union {<<delivery, response>>}
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_delivery_deliverEnabled(delivery) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ state_CallbackDelivery[delivery] = "pending"

Callback_delivery_deliver(delivery) ==
    /\ Callback_delivery_deliverEnabled(delivery)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "delivered"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_delivery_enqueueEnabled(callback, delivery) ==
    /\ callback \in CallbackIDs
    /\ callback \in exists_Callback
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \notin exists_CallbackDelivery

Callback_delivery_enqueue(callback, delivery) ==
    /\ Callback_delivery_enqueueEnabled(callback, delivery)
    /\ exists_CallbackDelivery' = exists_CallbackDelivery \union {delivery}
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "pending"]
    /\ relation_callback_delivery' = relation_callback_delivery \union {<<callback, delivery>>}
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_delivery_fail_deliveredEnabled(delivery) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ state_CallbackDelivery[delivery] = "delivered"

Callback_delivery_fail_delivered(delivery) ==
    /\ Callback_delivery_fail_deliveredEnabled(delivery)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_delivery_fail_pendingEnabled(delivery) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ state_CallbackDelivery[delivery] = "pending"

Callback_delivery_fail_pending(delivery) ==
    /\ Callback_delivery_fail_pendingEnabled(delivery)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_delivery_retryEnabled(delivery) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ state_CallbackDelivery[delivery] = "failed"

Callback_delivery_retry(delivery) ==
    /\ Callback_delivery_retryEnabled(delivery)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "pending"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_handler_close_cancelEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ \A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => \A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))

Callback_handler_close_cancel(entity) ==
    /\ Callback_handler_close_cancelEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_handler_close_completeEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ \A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => \A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))

Callback_handler_close_complete(entity) ==
    /\ Callback_handler_close_completeEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "completed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_handler_close_continue_as_newEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ \A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => \A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))

Callback_handler_close_continue_as_new(entity) ==
    /\ Callback_handler_close_continue_as_newEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "continued_as_new"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_handler_close_failEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ \A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => \A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))

Callback_handler_close_fail(entity) ==
    /\ Callback_handler_close_failEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_handler_close_terminateEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ \A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => \A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))

Callback_handler_close_terminate(entity) ==
    /\ Callback_handler_close_terminateEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_handler_close_timeoutEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ \A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => \A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))

Callback_handler_close_timeout(entity) ==
    /\ Callback_handler_close_timeoutEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Callback_handler_startEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \notin exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "created"

Callback_handler_start(entity) ==
    /\ Callback_handler_startEnabled(entity)
    /\ exists_WorkflowRun' = exists_WorkflowRun \union {entity}
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "started"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_NexusOperation, state_NexusOperation, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run, relation_callback_operation, relation_nexus_operation_handler_run>>

Next ==
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
    \/ \E callback \in CallbackIDs, handlerRun \in WorkflowRunIDs: Callback_attach_handler(callback, handlerRun)
    \/ \E callback \in CallbackIDs, operation \in NexusOperationIDs, handlerRun \in WorkflowRunIDs: Callback_attach_reference(callback, operation, handlerRun)
    \/ \E delivery \in CallbackDeliveryIDs, response \in CallbackResponseIDs: Callback_delivery_acknowledge(delivery, response)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_deliver(delivery)
    \/ \E callback \in CallbackIDs, delivery \in CallbackDeliveryIDs: Callback_delivery_enqueue(callback, delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_fail_delivered(delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_fail_pending(delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_retry(delivery)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_cancel(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_complete(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_continue_as_new(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_fail(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_terminate(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_timeout(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_start(entity)

CanStep ==
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
    \/ \E callback \in CallbackIDs, handlerRun \in WorkflowRunIDs: Callback_attach_handlerEnabled(callback, handlerRun)
    \/ \E callback \in CallbackIDs, operation \in NexusOperationIDs, handlerRun \in WorkflowRunIDs: Callback_attach_referenceEnabled(callback, operation, handlerRun)
    \/ \E delivery \in CallbackDeliveryIDs, response \in CallbackResponseIDs: Callback_delivery_acknowledgeEnabled(delivery, response)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_deliverEnabled(delivery)
    \/ \E callback \in CallbackIDs, delivery \in CallbackDeliveryIDs: Callback_delivery_enqueueEnabled(callback, delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_fail_deliveredEnabled(delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_fail_pendingEnabled(delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_retryEnabled(delivery)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_cancelEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_completeEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_continue_as_newEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_failEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_terminateEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_timeoutEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_startEnabled(entity)

CallbackHandlerLifetime ==
    \A handlerRun \in WorkflowRunIDs: handlerRun \in exists_WorkflowRun => (((state_WorkflowRun[handlerRun] = "completed" \/ state_WorkflowRun[handlerRun] = "failed" \/ state_WorkflowRun[handlerRun] = "canceled" \/ state_WorkflowRun[handlerRun] = "terminated" \/ state_WorkflowRun[handlerRun] = "timed_out" \/ state_WorkflowRun[handlerRun] = "continued_as_new") => \A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, handlerRun>> \in relation_callback_handler_run => \A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged"))))))

CallbackReferenceConsistency ==
    \A callback \in CallbackIDs: callback \in exists_Callback => (\A operation \in NexusOperationIDs: operation \in exists_NexusOperation => (\A handlerRun \in WorkflowRunIDs: handlerRun \in exists_WorkflowRun => (((<<callback, operation>> \in relation_callback_operation /\ <<callback, handlerRun>> \in relation_callback_handler_run) => <<operation, handlerRun>> \in relation_nexus_operation_handler_run))))

CallbackResponseConsistency ==
    (\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((state_CallbackDelivery[delivery] = "acknowledged" => \E response \in CallbackResponseIDs: response \in exists_CallbackResponse /\ ((<<delivery, response>> \in relation_callback_delivery_response /\ state_CallbackResponse[response] = "accepted")))) /\ \A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => (\A response \in CallbackResponseIDs: response \in exists_CallbackResponse => ((<<delivery, response>> \in relation_callback_delivery_response => state_CallbackResponse[response] = "accepted"))))

InductiveInvariant ==
    /\ TypeOK
    /\ Cardinality_callback_delivery
    /\ Cardinality_callback_delivery_response
    /\ Cardinality_callback_handler_run
    /\ Cardinality_callback_operation
    /\ Cardinality_nexus_operation_handler_run
    /\ CallbackHandlerLifetime
    /\ CallbackReferenceConsistency
    /\ CallbackResponseConsistency
DeclaredSafety ==
    /\ CallbackHandlerLifetime
    /\ CallbackReferenceConsistency
    /\ CallbackResponseConsistency
Safety == InductiveInvariant /\ DeclaredSafety

QuiescentSafety == TRUE

Spec == Init /\ [][Next]_vars

====
