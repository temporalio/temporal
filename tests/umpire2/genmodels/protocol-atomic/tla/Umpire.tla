---- MODULE Umpire ----
EXTENDS FiniteSets, Naturals

CONSTANTS
    \* @type: Set(Str);
    ActivityIDs,
    \* @type: Set(Str);
    CallbackIDs,
    \* @type: Set(Str);
    NexusOperationIDs,
    \* @type: Set(Str);
    TaskQueueIDs,
    \* @type: Set(Str);
    WorkflowIDs,
    \* @type: Set(Str);
    WorkflowRunIDs,
    \* @type: Set(Str);
    WorkflowTaskIDs

VARIABLES
    \* @type: Set(Str);
    exists_Activity,
    \* @type: Str -> Str;
    state_Activity,
    \* @type: Set(Str);
    exists_Callback,
    \* @type: Str -> Str;
    state_Callback,
    \* @type: Set(Str);
    exists_NexusOperation,
    \* @type: Str -> Str;
    state_NexusOperation,
    \* @type: Set(Str);
    exists_TaskQueue,
    \* @type: Set(Str);
    exists_Workflow,
    \* @type: Str -> Str;
    state_Workflow,
    \* @type: Set(Str);
    exists_WorkflowRun,
    \* @type: Str -> Str;
    state_WorkflowRun,
    \* @type: Set(Str);
    exists_WorkflowTask,
    \* @type: Str -> Str;
    state_WorkflowTask,
    \* @type: Set(<<Str, Str>>);
    relation_activity_nexus,
    \* @type: Set(<<Str, Str>>);
    relation_callback_handler_run,
    \* @type: Set(<<Str, Str>>);
    relation_callback_operation,
    \* @type: Set(<<Str, Str>>);
    relation_nexus_activity,
    \* @type: Set(<<Str, Str>>);
    relation_workflow_run_successor,
    \* @type: Set(<<Str, Str>>);
    relation_workflow_runs

vars == <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

TypeOK ==
    /\ exists_Activity \in SUBSET ActivityIDs
    /\ state_Activity \in [ActivityIDs -> {"backing_off", "canceled", "completed", "failed", "scheduled", "started", "timed_out", "unspecified"}]
    /\ exists_Callback \in SUBSET CallbackIDs
    /\ state_Callback \in [CallbackIDs -> {"unobserved"}]
    /\ exists_NexusOperation \in SUBSET NexusOperationIDs
    /\ state_NexusOperation \in [NexusOperationIDs -> {"backing_off", "canceled", "failed", "rejected", "scheduled", "started", "succeeded", "terminated", "timed_out", "unspecified"}]
    /\ exists_TaskQueue \in SUBSET TaskQueueIDs
    /\ exists_Workflow \in SUBSET WorkflowIDs
    /\ state_Workflow \in [WorkflowIDs -> {"canceled", "completed", "created", "failed", "started", "terminated", "timed_out"}]
    /\ exists_WorkflowRun \in SUBSET WorkflowRunIDs
    /\ state_WorkflowRun \in [WorkflowRunIDs -> {"canceled", "completed", "continued_as_new", "created", "failed", "started", "terminated", "timed_out"}]
    /\ exists_WorkflowTask \in SUBSET WorkflowTaskIDs
    /\ state_WorkflowTask \in [WorkflowTaskIDs -> {"added", "created", "discarded", "polled", "stored", "terminated"}]
    /\ relation_activity_nexus \in SUBSET (ActivityIDs \X NexusOperationIDs)
    /\ relation_callback_handler_run \in SUBSET (CallbackIDs \X WorkflowRunIDs)
    /\ relation_callback_operation \in SUBSET (CallbackIDs \X NexusOperationIDs)
    /\ relation_nexus_activity \in SUBSET (NexusOperationIDs \X ActivityIDs)
    /\ relation_workflow_run_successor \in SUBSET (WorkflowRunIDs \X WorkflowRunIDs)
    /\ relation_workflow_runs \in SUBSET (WorkflowIDs \X WorkflowRunIDs)

Cardinality_activity_nexus ==
    /\ \A tuple \in relation_activity_nexus: tuple[1] \in exists_Activity /\ tuple[2] \in exists_NexusOperation
    /\ \A source \in ActivityIDs: Cardinality({target \in NexusOperationIDs: <<source, target>> \in relation_activity_nexus}) <= 1
    /\ \A target \in NexusOperationIDs: Cardinality({source \in ActivityIDs: <<source, target>> \in relation_activity_nexus}) <= 1

Cardinality_callback_handler_run ==
    /\ \A tuple \in relation_callback_handler_run: tuple[1] \in exists_Callback /\ tuple[2] \in exists_WorkflowRun
    /\ \A source \in CallbackIDs: Cardinality({target \in WorkflowRunIDs: <<source, target>> \in relation_callback_handler_run}) <= 1

Cardinality_callback_operation ==
    /\ \A tuple \in relation_callback_operation: tuple[1] \in exists_Callback /\ tuple[2] \in exists_NexusOperation
    /\ \A source \in CallbackIDs: Cardinality({target \in NexusOperationIDs: <<source, target>> \in relation_callback_operation}) <= 1

Cardinality_nexus_activity ==
    /\ \A tuple \in relation_nexus_activity: tuple[1] \in exists_NexusOperation /\ tuple[2] \in exists_Activity
    /\ \A source \in NexusOperationIDs: Cardinality({target \in ActivityIDs: <<source, target>> \in relation_nexus_activity}) <= 1
    /\ \A target \in ActivityIDs: Cardinality({source \in NexusOperationIDs: <<source, target>> \in relation_nexus_activity}) <= 1

Cardinality_workflow_run_successor ==
    /\ \A tuple \in relation_workflow_run_successor: tuple[1] \in exists_WorkflowRun /\ tuple[2] \in exists_WorkflowRun
    /\ \A source \in WorkflowRunIDs: Cardinality({target \in WorkflowRunIDs: <<source, target>> \in relation_workflow_run_successor}) <= 1
    /\ \A target \in WorkflowRunIDs: Cardinality({source \in WorkflowRunIDs: <<source, target>> \in relation_workflow_run_successor}) <= 1

Cardinality_workflow_runs ==
    /\ \A tuple \in relation_workflow_runs: tuple[1] \in exists_Workflow /\ tuple[2] \in exists_WorkflowRun
    /\ \A target \in WorkflowRunIDs: Cardinality({source \in WorkflowIDs: <<source, target>> \in relation_workflow_runs}) <= 1

Init ==
    /\ exists_Activity = {}
    /\ state_Activity = [entity \in ActivityIDs |-> "unspecified"]
    /\ exists_Callback = {}
    /\ state_Callback = [entity \in CallbackIDs |-> "unobserved"]
    /\ exists_NexusOperation = {}
    /\ state_NexusOperation = [entity \in NexusOperationIDs |-> "unspecified"]
    /\ exists_TaskQueue = {}
    /\ exists_Workflow = {}
    /\ state_Workflow = [entity \in WorkflowIDs |-> "created"]
    /\ exists_WorkflowRun = {}
    /\ state_WorkflowRun = [entity \in WorkflowRunIDs |-> "created"]
    /\ exists_WorkflowTask = {}
    /\ state_WorkflowTask = [entity \in WorkflowTaskIDs |-> "created"]
    /\ relation_activity_nexus = {}
    /\ relation_callback_handler_run = {}
    /\ relation_callback_operation = {}
    /\ relation_nexus_activity = {}
    /\ relation_workflow_run_successor = {}
    /\ relation_workflow_runs = {}

Activity_backing_off_cancel_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "backing_off"

Activity_backing_off_cancel_AnyHosting(entity) ==
    /\ Activity_backing_off_cancel_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_backing_off_schedule_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "backing_off"

Activity_backing_off_schedule_AnyHosting(entity) ==
    /\ Activity_backing_off_schedule_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_backing_off_timeout_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "backing_off"

Activity_backing_off_timeout_AnyHosting(entity) ==
    /\ Activity_backing_off_timeout_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_scheduled_cancel_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "scheduled"

Activity_scheduled_cancel_AnyHosting(entity) ==
    /\ Activity_scheduled_cancel_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_scheduled_fail_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "scheduled"

Activity_scheduled_fail_AnyHosting(entity) ==
    /\ Activity_scheduled_fail_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_scheduled_start_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "scheduled"

Activity_scheduled_start_AnyHosting(entity) ==
    /\ Activity_scheduled_start_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "started"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_scheduled_timeout_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "scheduled"

Activity_scheduled_timeout_AnyHosting(entity) ==
    /\ Activity_scheduled_timeout_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_started_attempt_failed_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_attempt_failed_AnyHosting(entity) ==
    /\ Activity_started_attempt_failed_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "backing_off"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_started_cancel_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_cancel_AnyHosting(entity) ==
    /\ Activity_started_cancel_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_started_complete_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_complete_AnyHosting(entity) ==
    /\ Activity_started_complete_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "completed"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_started_fail_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_fail_AnyHosting(entity) ==
    /\ Activity_started_fail_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_started_timeout_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \in exists_Activity
    /\ state_Activity[entity] = "started"

Activity_started_timeout_AnyHosting(entity) ==
    /\ Activity_started_timeout_AnyHostingEnabled(entity)
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Activity_unspecified_schedule_AnyHostingEnabled(entity) ==
    /\ entity \in ActivityIDs
    /\ entity \notin exists_Activity
    /\ state_Activity[entity] = "unspecified"

Activity_unspecified_schedule_AnyHosting(entity) ==
    /\ Activity_unspecified_schedule_AnyHostingEnabled(entity)
    /\ exists_Activity' = exists_Activity \union {entity}
    /\ state_Activity' = [state_Activity EXCEPT ![entity] = "scheduled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_backing_off_schedule_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_schedule_Embedded(op) ==
    /\ NexusOperation_backing_off_schedule_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_backing_off_schedule_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_schedule_Standalone(op) ==
    /\ NexusOperation_backing_off_schedule_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_backing_off_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "backing_off"

NexusOperation_backing_off_terminate_Embedded(entity) ==
    /\ NexusOperation_backing_off_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_backing_off_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_terminate_Standalone(op) ==
    /\ NexusOperation_backing_off_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_backing_off_timeout_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_timeout_Embedded(op) ==
    /\ NexusOperation_backing_off_timeout_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_backing_off_timeout_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "backing_off"

NexusOperation_backing_off_timeout_Standalone(op) ==
    /\ NexusOperation_backing_off_timeout_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_attempt_failed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_attempt_failed_Embedded(op) ==
    /\ NexusOperation_scheduled_attempt_failed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "backing_off"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_attempt_failed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_attempt_failed_Standalone(op) ==
    /\ NexusOperation_scheduled_attempt_failed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "backing_off"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_cancel_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_cancel_Embedded(op) ==
    /\ NexusOperation_scheduled_cancel_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_cancel_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_cancel_Standalone(op) ==
    /\ NexusOperation_scheduled_cancel_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_fail_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_fail_Embedded(op) ==
    /\ NexusOperation_scheduled_fail_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_fail_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_fail_Standalone(op) ==
    /\ NexusOperation_scheduled_fail_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_start_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_start_Embedded(op) ==
    /\ NexusOperation_scheduled_start_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "started"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_start_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_start_Standalone(op) ==
    /\ NexusOperation_scheduled_start_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "started"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_succeed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_succeed_Embedded(op) ==
    /\ NexusOperation_scheduled_succeed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_succeed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_succeed_Standalone(op) ==
    /\ NexusOperation_scheduled_succeed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "scheduled"

NexusOperation_scheduled_terminate_Embedded(entity) ==
    /\ NexusOperation_scheduled_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_terminate_Standalone(op) ==
    /\ NexusOperation_scheduled_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_timeout_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_timeout_Embedded(op) ==
    /\ NexusOperation_scheduled_timeout_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_scheduled_timeout_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "scheduled"

NexusOperation_scheduled_timeout_Standalone(op) ==
    /\ NexusOperation_scheduled_timeout_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_cancel_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_cancel_Embedded(op) ==
    /\ NexusOperation_started_cancel_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_cancel_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_cancel_Standalone(op) ==
    /\ NexusOperation_started_cancel_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_fail_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_fail_Embedded(op) ==
    /\ NexusOperation_started_fail_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_fail_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_fail_Standalone(op) ==
    /\ NexusOperation_started_fail_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_succeed_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_succeed_Embedded(op) ==
    /\ NexusOperation_started_succeed_EmbeddedEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_succeed_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_succeed_Standalone(op) ==
    /\ NexusOperation_started_succeed_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "succeeded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_terminate_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_terminate_Embedded(entity) ==
    /\ NexusOperation_started_terminate_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_terminate_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \in exists_NexusOperation
    /\ state_NexusOperation[op] = "started"

NexusOperation_started_terminate_Standalone(op) ==
    /\ NexusOperation_started_terminate_StandaloneEnabled(op)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_timeout_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_timeout_Embedded(entity) ==
    /\ NexusOperation_started_timeout_EmbeddedEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_started_timeout_StandaloneEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \in exists_NexusOperation
    /\ state_NexusOperation[entity] = "started"

NexusOperation_started_timeout_Standalone(entity) ==
    /\ NexusOperation_started_timeout_StandaloneEnabled(entity)
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_unspecified_reject_EmbeddedEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \notin exists_NexusOperation
    /\ state_NexusOperation[entity] = "unspecified"

NexusOperation_unspecified_reject_Embedded(entity) ==
    /\ NexusOperation_unspecified_reject_EmbeddedEnabled(entity)
    /\ exists_NexusOperation' = exists_NexusOperation \union {entity}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "rejected"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_unspecified_reject_StandaloneEnabled(entity) ==
    /\ entity \in NexusOperationIDs
    /\ entity \notin exists_NexusOperation
    /\ state_NexusOperation[entity] = "unspecified"

NexusOperation_unspecified_reject_Standalone(entity) ==
    /\ NexusOperation_unspecified_reject_StandaloneEnabled(entity)
    /\ exists_NexusOperation' = exists_NexusOperation \union {entity}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![entity] = "rejected"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_unspecified_schedule_EmbeddedEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \notin exists_NexusOperation
    /\ state_NexusOperation[op] = "unspecified"

NexusOperation_unspecified_schedule_Embedded(op) ==
    /\ NexusOperation_unspecified_schedule_EmbeddedEnabled(op)
    /\ exists_NexusOperation' = exists_NexusOperation \union {op}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

NexusOperation_unspecified_schedule_StandaloneEnabled(op) ==
    /\ op \in NexusOperationIDs
    /\ op \notin exists_NexusOperation
    /\ state_NexusOperation[op] = "unspecified"

NexusOperation_unspecified_schedule_Standalone(op) ==
    /\ NexusOperation_unspecified_schedule_StandaloneEnabled(op)
    /\ exists_NexusOperation' = exists_NexusOperation \union {op}
    /\ state_NexusOperation' = [state_NexusOperation EXCEPT ![op] = "scheduled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Workflow_created_start_StandaloneEnabled(wf) ==
    /\ wf \in WorkflowIDs
    /\ wf \notin exists_Workflow
    /\ state_Workflow[wf] = "created"

Workflow_created_start_Standalone(wf) ==
    /\ Workflow_created_start_StandaloneEnabled(wf)
    /\ exists_Workflow' = exists_Workflow \union {wf}
    /\ state_Workflow' = [state_Workflow EXCEPT ![wf] = "started"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Workflow_started_cancel_StandaloneEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ state_Workflow[entity] = "started"

Workflow_started_cancel_Standalone(entity) ==
    /\ Workflow_started_cancel_StandaloneEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Workflow_started_complete_StandaloneEnabled(wf) ==
    /\ wf \in WorkflowIDs
    /\ wf \in exists_Workflow
    /\ state_Workflow[wf] = "started"

Workflow_started_complete_Standalone(wf) ==
    /\ Workflow_started_complete_StandaloneEnabled(wf)
    /\ state_Workflow' = [state_Workflow EXCEPT ![wf] = "completed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Workflow_started_fail_StandaloneEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ state_Workflow[entity] = "started"

Workflow_started_fail_Standalone(entity) ==
    /\ Workflow_started_fail_StandaloneEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Workflow_started_terminate_StandaloneEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ state_Workflow[entity] = "started"

Workflow_started_terminate_Standalone(entity) ==
    /\ Workflow_started_terminate_StandaloneEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

Workflow_started_timeout_StandaloneEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ state_Workflow[entity] = "started"

Workflow_started_timeout_Standalone(entity) ==
    /\ Workflow_started_timeout_StandaloneEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowRun_created_start_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \notin exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "created"

WorkflowRun_created_start_AnyHosting(entity) ==
    /\ WorkflowRun_created_start_AnyHostingEnabled(entity)
    /\ exists_WorkflowRun' = exists_WorkflowRun \union {entity}
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "started"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowRun_started_cancel_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_cancel_AnyHosting(entity) ==
    /\ WorkflowRun_started_cancel_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowRun_started_complete_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_complete_AnyHosting(entity) ==
    /\ WorkflowRun_started_complete_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "completed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowRun_started_continue_as_new_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_continue_as_new_AnyHosting(entity) ==
    /\ WorkflowRun_started_continue_as_new_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "continued_as_new"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowRun_started_fail_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_fail_AnyHosting(entity) ==
    /\ WorkflowRun_started_fail_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowRun_started_terminate_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_terminate_AnyHosting(entity) ==
    /\ WorkflowRun_started_terminate_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowRun_started_timeout_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_timeout_AnyHosting(entity) ==
    /\ WorkflowRun_started_timeout_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowTask_added_discard_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "added"

WorkflowTask_added_discard_AnyHosting(entity) ==
    /\ WorkflowTask_added_discard_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "discarded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowTask_added_poll_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "added"

WorkflowTask_added_poll_AnyHosting(entity) ==
    /\ WorkflowTask_added_poll_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "polled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowTask_added_store_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "added"

WorkflowTask_added_store_AnyHosting(entity) ==
    /\ WorkflowTask_added_store_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "stored"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowTask_added_terminate_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "added"

WorkflowTask_added_terminate_AnyHosting(entity) ==
    /\ WorkflowTask_added_terminate_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowTask_created_add_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \notin exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "created"

WorkflowTask_created_add_AnyHosting(entity) ==
    /\ WorkflowTask_created_add_AnyHostingEnabled(entity)
    /\ exists_WorkflowTask' = exists_WorkflowTask \union {entity}
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "added"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowTask_created_terminate_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \notin exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "created"

WorkflowTask_created_terminate_AnyHosting(entity) ==
    /\ WorkflowTask_created_terminate_AnyHostingEnabled(entity)
    /\ exists_WorkflowTask' = exists_WorkflowTask \union {entity}
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowTask_stored_discard_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "stored"

WorkflowTask_stored_discard_AnyHosting(entity) ==
    /\ WorkflowTask_stored_discard_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "discarded"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowTask_stored_poll_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "stored"

WorkflowTask_stored_poll_AnyHosting(entity) ==
    /\ WorkflowTask_stored_poll_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "polled"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

WorkflowTask_stored_terminate_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "stored"

WorkflowTask_stored_terminate_AnyHosting(entity) ==
    /\ WorkflowTask_stored_terminate_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Activity, state_Activity, exists_Callback, state_Callback, exists_NexusOperation, state_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_activity_nexus, relation_callback_handler_run, relation_callback_operation, relation_nexus_activity, relation_workflow_run_successor, relation_workflow_runs>>

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
    /\ relation_activity_nexus' = relation_activity_nexus \union {<<activity, operation>>}
    /\ relation_nexus_activity' = relation_nexus_activity \union {<<operation, activity>>}
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_NexusOperation, exists_TaskQueue, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_callback_handler_run, relation_callback_operation, relation_workflow_run_successor, relation_workflow_runs>>

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
    \/ \E wf \in WorkflowIDs: Workflow_created_start_Standalone(wf)
    \/ \E entity \in WorkflowIDs: Workflow_started_cancel_Standalone(entity)
    \/ \E wf \in WorkflowIDs: Workflow_started_complete_Standalone(wf)
    \/ \E entity \in WorkflowIDs: Workflow_started_fail_Standalone(entity)
    \/ \E entity \in WorkflowIDs: Workflow_started_terminate_Standalone(entity)
    \/ \E entity \in WorkflowIDs: Workflow_started_timeout_Standalone(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_created_start_AnyHosting(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_cancel_AnyHosting(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_complete_AnyHosting(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_continue_as_new_AnyHosting(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_fail_AnyHosting(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_terminate_AnyHosting(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_timeout_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_discard_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_poll_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_store_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_terminate_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_created_add_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_created_terminate_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_discard_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_poll_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_terminate_AnyHosting(entity)
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
    \/ \E wf \in WorkflowIDs: Workflow_created_start_StandaloneEnabled(wf)
    \/ \E entity \in WorkflowIDs: Workflow_started_cancel_StandaloneEnabled(entity)
    \/ \E wf \in WorkflowIDs: Workflow_started_complete_StandaloneEnabled(wf)
    \/ \E entity \in WorkflowIDs: Workflow_started_fail_StandaloneEnabled(entity)
    \/ \E entity \in WorkflowIDs: Workflow_started_terminate_StandaloneEnabled(entity)
    \/ \E entity \in WorkflowIDs: Workflow_started_timeout_StandaloneEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_created_start_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_cancel_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_complete_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_continue_as_new_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_fail_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_terminate_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: WorkflowRun_started_timeout_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_discard_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_poll_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_store_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_terminate_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_created_add_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_created_terminate_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_discard_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_poll_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_terminate_AnyHostingEnabled(entity)
    \/ \E activity \in ActivityIDs, operation \in NexusOperationIDs: Regression_nexus_start_activityEnabled(activity, operation)

Activity_backing_off_quiescent_progress ==
    \A entity \in ActivityIDs: entity \in exists_Activity => (~(state_Activity[entity] = "backing_off"))

Activity_scheduled_quiescent_progress ==
    \A entity \in ActivityIDs: entity \in exists_Activity => (~(state_Activity[entity] = "scheduled"))

Activity_started_quiescent_progress ==
    \A entity \in ActivityIDs: entity \in exists_Activity => (~(state_Activity[entity] = "started"))

NexusActivityForwardLinkConsistency ==
    \A source \in NexusOperationIDs: source \in exists_NexusOperation => (\A target \in ActivityIDs: target \in exists_Activity => ((<<source, target>> \in relation_nexus_activity => <<target, source>> \in relation_activity_nexus)))

NexusActivityReverseLinkConsistency ==
    \A source \in ActivityIDs: source \in exists_Activity => (\A target \in NexusOperationIDs: target \in exists_NexusOperation => ((<<source, target>> \in relation_activity_nexus => <<target, source>> \in relation_nexus_activity)))

NexusActivityTerminalRefinement ==
    \A operation \in NexusOperationIDs: operation \in exists_NexusOperation => (\A activity \in ActivityIDs: activity \in exists_Activity => ((<<operation, activity>> \in relation_nexus_activity => (state_NexusOperation[operation] = "succeeded" /\ state_Activity[activity] = "completed"))))

NexusOperation_backing_off_quiescent_progress ==
    \A entity \in NexusOperationIDs: entity \in exists_NexusOperation => (~(state_NexusOperation[entity] = "backing_off"))

NexusOperation_scheduled_quiescent_progress ==
    \A entity \in NexusOperationIDs: entity \in exists_NexusOperation => (~(state_NexusOperation[entity] = "scheduled"))

NexusOperation_started_quiescent_progress ==
    \A entity \in NexusOperationIDs: entity \in exists_NexusOperation => (~(state_NexusOperation[entity] = "started"))

Workflow_started_quiescent_progress ==
    \A entity \in WorkflowIDs: entity \in exists_Workflow => (~(state_Workflow[entity] = "started"))

InductiveInvariant ==
    /\ TypeOK
    /\ Cardinality_activity_nexus
    /\ Cardinality_callback_handler_run
    /\ Cardinality_callback_operation
    /\ Cardinality_nexus_activity
    /\ Cardinality_workflow_run_successor
    /\ Cardinality_workflow_runs
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
       /\ Workflow_started_quiescent_progress

Spec == Init /\ [][Next]_vars

====
