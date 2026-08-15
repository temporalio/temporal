---- MODULE Umpire ----
EXTENDS FiniteSets, Naturals

CONSTANTS
    \* @type: Set(Str);
    DeliveryAttemptIDs,
    \* @type: Set(Str);
    DeliveryQueueIDs,
    \* @type: Set(Str);
    DeliveryTaskIDs,
    \* @type: Set(Str);
    PollerIDs,
    \* @type: Set(Str);
    WorkObligationIDs,
    \* @type: Set(Str);
    WorkflowIDs,
    \* @type: Set(Str);
    WorkflowRunIDs,
    \* @type: Set(Str);
    WorkflowTaskIDs

VARIABLES
    \* @type: Set(Str);
    exists_DeliveryAttempt,
    \* @type: Str -> Str;
    state_DeliveryAttempt,
    \* @type: Set(Str);
    exists_DeliveryQueue,
    \* @type: Str -> Str;
    state_DeliveryQueue,
    \* @type: Set(Str);
    exists_DeliveryTask,
    \* @type: Str -> Str;
    state_DeliveryTask,
    \* @type: Set(Str);
    exists_Poller,
    \* @type: Str -> Str;
    state_Poller,
    \* @type: Set(Str);
    exists_WorkObligation,
    \* @type: Str -> Str;
    state_WorkObligation,
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
    relation_delivery_accepted_start,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_attempt_poller,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_attempt_task,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_obligation,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_queue,
    \* @type: Set(<<Str, Str>>);
    relation_workflow_run_successor,
    \* @type: Set(<<Str, Str>>);
    relation_workflow_runs,
    \* @type: Set(<<Str, Str>>);
    relation_workflow_task_delivery_task,
    \* @type: Set(<<Str, Str>>);
    relation_workflow_task_obligation

vars == <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

TypeOK ==
    /\ exists_DeliveryAttempt \in SUBSET DeliveryAttemptIDs
    /\ state_DeliveryAttempt \in [DeliveryAttemptIDs -> {"accepted", "completed", "dispatched", "failed", "rejected", "reserved"}]
    /\ exists_DeliveryQueue \in SUBSET DeliveryQueueIDs
    /\ state_DeliveryQueue \in [DeliveryQueueIDs -> {"available"}]
    /\ exists_DeliveryTask \in SUBSET DeliveryTaskIDs
    /\ state_DeliveryTask \in [DeliveryTaskIDs -> {"acknowledged", "authorized", "backlogged", "dispatched", "pending", "reserved", "retired", "sync-offered"}]
    /\ exists_Poller \in SUBSET PollerIDs
    /\ state_Poller \in [PollerIDs -> {"available"}]
    /\ exists_WorkObligation \in SUBSET WorkObligationIDs
    /\ state_WorkObligation \in [WorkObligationIDs -> {"accepted", "terminal", "unresolved", "valid"}]
    /\ exists_Workflow \in SUBSET WorkflowIDs
    /\ state_Workflow \in [WorkflowIDs -> {"canceled", "completed", "created", "failed", "started", "terminated", "timed_out"}]
    /\ exists_WorkflowRun \in SUBSET WorkflowRunIDs
    /\ state_WorkflowRun \in [WorkflowRunIDs -> {"canceled", "completed", "continued_as_new", "created", "failed", "started", "terminated", "timed_out"}]
    /\ exists_WorkflowTask \in SUBSET WorkflowTaskIDs
    /\ state_WorkflowTask \in [WorkflowTaskIDs -> {"added", "created", "discarded", "polled", "stored", "terminated"}]
    /\ relation_delivery_accepted_start \in SUBSET (WorkObligationIDs \X DeliveryAttemptIDs)
    /\ relation_delivery_attempt_poller \in SUBSET (DeliveryAttemptIDs \X PollerIDs)
    /\ relation_delivery_attempt_task \in SUBSET (DeliveryAttemptIDs \X DeliveryTaskIDs)
    /\ relation_delivery_task_obligation \in SUBSET (DeliveryTaskIDs \X WorkObligationIDs)
    /\ relation_delivery_task_queue \in SUBSET (DeliveryTaskIDs \X DeliveryQueueIDs)
    /\ relation_workflow_run_successor \in SUBSET (WorkflowRunIDs \X WorkflowRunIDs)
    /\ relation_workflow_runs \in SUBSET (WorkflowIDs \X WorkflowRunIDs)
    /\ relation_workflow_task_delivery_task \in SUBSET (WorkflowTaskIDs \X DeliveryTaskIDs)
    /\ relation_workflow_task_obligation \in SUBSET (WorkflowTaskIDs \X WorkObligationIDs)

Cardinality_delivery_accepted_start ==
    /\ \A tuple \in relation_delivery_accepted_start: tuple[1] \in exists_WorkObligation /\ tuple[2] \in exists_DeliveryAttempt
    /\ \A source \in WorkObligationIDs: Cardinality({target \in DeliveryAttemptIDs: <<source, target>> \in relation_delivery_accepted_start}) <= 1
    /\ \A target \in DeliveryAttemptIDs: Cardinality({source \in WorkObligationIDs: <<source, target>> \in relation_delivery_accepted_start}) <= 1

Cardinality_delivery_attempt_poller ==
    /\ \A tuple \in relation_delivery_attempt_poller: tuple[1] \in exists_DeliveryAttempt /\ tuple[2] \in exists_Poller
    /\ \A source \in DeliveryAttemptIDs: Cardinality({target \in PollerIDs: <<source, target>> \in relation_delivery_attempt_poller}) <= 1

Cardinality_delivery_attempt_task ==
    /\ \A tuple \in relation_delivery_attempt_task: tuple[1] \in exists_DeliveryAttempt /\ tuple[2] \in exists_DeliveryTask
    /\ \A source \in DeliveryAttemptIDs: Cardinality({target \in DeliveryTaskIDs: <<source, target>> \in relation_delivery_attempt_task}) <= 1

Cardinality_delivery_task_obligation ==
    /\ \A tuple \in relation_delivery_task_obligation: tuple[1] \in exists_DeliveryTask /\ tuple[2] \in exists_WorkObligation
    /\ \A source \in DeliveryTaskIDs: Cardinality({target \in WorkObligationIDs: <<source, target>> \in relation_delivery_task_obligation}) <= 1

Cardinality_delivery_task_queue ==
    /\ \A tuple \in relation_delivery_task_queue: tuple[1] \in exists_DeliveryTask /\ tuple[2] \in exists_DeliveryQueue
    /\ \A source \in DeliveryTaskIDs: Cardinality({target \in DeliveryQueueIDs: <<source, target>> \in relation_delivery_task_queue}) <= 1

Cardinality_workflow_run_successor ==
    /\ \A tuple \in relation_workflow_run_successor: tuple[1] \in exists_WorkflowRun /\ tuple[2] \in exists_WorkflowRun
    /\ \A source \in WorkflowRunIDs: Cardinality({target \in WorkflowRunIDs: <<source, target>> \in relation_workflow_run_successor}) <= 1
    /\ \A target \in WorkflowRunIDs: Cardinality({source \in WorkflowRunIDs: <<source, target>> \in relation_workflow_run_successor}) <= 1

Cardinality_workflow_runs ==
    /\ \A tuple \in relation_workflow_runs: tuple[1] \in exists_Workflow /\ tuple[2] \in exists_WorkflowRun
    /\ \A target \in WorkflowRunIDs: Cardinality({source \in WorkflowIDs: <<source, target>> \in relation_workflow_runs}) <= 1

Cardinality_workflow_task_delivery_task ==
    /\ \A tuple \in relation_workflow_task_delivery_task: tuple[1] \in exists_WorkflowTask /\ tuple[2] \in exists_DeliveryTask
    /\ \A source \in WorkflowTaskIDs: Cardinality({target \in DeliveryTaskIDs: <<source, target>> \in relation_workflow_task_delivery_task}) <= 1
    /\ \A target \in DeliveryTaskIDs: Cardinality({source \in WorkflowTaskIDs: <<source, target>> \in relation_workflow_task_delivery_task}) <= 1

Cardinality_workflow_task_obligation ==
    /\ \A tuple \in relation_workflow_task_obligation: tuple[1] \in exists_WorkflowTask /\ tuple[2] \in exists_WorkObligation
    /\ \A source \in WorkflowTaskIDs: Cardinality({target \in WorkObligationIDs: <<source, target>> \in relation_workflow_task_obligation}) <= 1
    /\ \A target \in WorkObligationIDs: Cardinality({source \in WorkflowTaskIDs: <<source, target>> \in relation_workflow_task_obligation}) <= 1

Init ==
    /\ exists_DeliveryAttempt = {}
    /\ state_DeliveryAttempt = [entity \in DeliveryAttemptIDs |-> "reserved"]
    /\ exists_DeliveryQueue = {"DeliveryQueue#0", "DeliveryQueue#1"}
    /\ state_DeliveryQueue = [entity \in DeliveryQueueIDs |-> "available"]
    /\ exists_DeliveryTask = {}
    /\ state_DeliveryTask = [entity \in DeliveryTaskIDs |-> "pending"]
    /\ exists_Poller = {"Poller#0", "Poller#1"}
    /\ state_Poller = [entity \in PollerIDs |-> "available"]
    /\ exists_WorkObligation = {}
    /\ state_WorkObligation = [entity \in WorkObligationIDs |-> "unresolved"]
    /\ exists_Workflow = {}
    /\ state_Workflow = [entity \in WorkflowIDs |-> "created"]
    /\ exists_WorkflowRun = {}
    /\ state_WorkflowRun = [entity \in WorkflowRunIDs |-> "created"]
    /\ exists_WorkflowTask = {}
    /\ state_WorkflowTask = [entity \in WorkflowTaskIDs |-> "created"]
    /\ relation_delivery_accepted_start = {}
    /\ relation_delivery_attempt_poller = {}
    /\ relation_delivery_attempt_task = {}
    /\ relation_delivery_task_obligation = {}
    /\ relation_delivery_task_queue = {}
    /\ relation_workflow_run_successor = {}
    /\ relation_workflow_runs = {}
    /\ relation_workflow_task_delivery_task = {}
    /\ relation_workflow_task_obligation = {}

Workflow_created_start_StandaloneEnabled(wf) ==
    /\ wf \in WorkflowIDs
    /\ wf \notin exists_Workflow
    /\ state_Workflow[wf] = "created"

Workflow_created_start_Standalone(wf) ==
    /\ Workflow_created_start_StandaloneEnabled(wf)
    /\ exists_Workflow' = exists_Workflow \union {wf}
    /\ state_Workflow' = [state_Workflow EXCEPT ![wf] = "started"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_started_cancel_StandaloneEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ state_Workflow[entity] = "started"

Workflow_started_cancel_Standalone(entity) ==
    /\ Workflow_started_cancel_StandaloneEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_started_complete_StandaloneEnabled(wf) ==
    /\ wf \in WorkflowIDs
    /\ wf \in exists_Workflow
    /\ state_Workflow[wf] = "started"

Workflow_started_complete_Standalone(wf) ==
    /\ Workflow_started_complete_StandaloneEnabled(wf)
    /\ state_Workflow' = [state_Workflow EXCEPT ![wf] = "completed"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_started_fail_StandaloneEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ state_Workflow[entity] = "started"

Workflow_started_fail_Standalone(entity) ==
    /\ Workflow_started_fail_StandaloneEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_started_terminate_StandaloneEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ state_Workflow[entity] = "started"

Workflow_started_terminate_Standalone(entity) ==
    /\ Workflow_started_terminate_StandaloneEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_started_timeout_StandaloneEnabled(entity) ==
    /\ entity \in WorkflowIDs
    /\ entity \in exists_Workflow
    /\ state_Workflow[entity] = "started"

Workflow_started_timeout_Standalone(entity) ==
    /\ Workflow_started_timeout_StandaloneEnabled(entity)
    /\ state_Workflow' = [state_Workflow EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowRun_created_start_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \notin exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "created"

WorkflowRun_created_start_AnyHosting(entity) ==
    /\ WorkflowRun_created_start_AnyHostingEnabled(entity)
    /\ exists_WorkflowRun' = exists_WorkflowRun \union {entity}
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "started"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowRun_started_cancel_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_cancel_AnyHosting(entity) ==
    /\ WorkflowRun_started_cancel_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowRun_started_complete_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_complete_AnyHosting(entity) ==
    /\ WorkflowRun_started_complete_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "completed"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowRun_started_continue_as_new_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_continue_as_new_AnyHosting(entity) ==
    /\ WorkflowRun_started_continue_as_new_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "continued_as_new"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowRun_started_fail_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_fail_AnyHosting(entity) ==
    /\ WorkflowRun_started_fail_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowRun_started_terminate_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_terminate_AnyHosting(entity) ==
    /\ WorkflowRun_started_terminate_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowRun_started_timeout_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "started"

WorkflowRun_started_timeout_AnyHosting(entity) ==
    /\ WorkflowRun_started_timeout_AnyHostingEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowTask_added_discard_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "added"

WorkflowTask_added_discard_AnyHosting(entity) ==
    /\ WorkflowTask_added_discard_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "discarded"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowTask_added_store_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "added"

WorkflowTask_added_store_AnyHosting(entity) ==
    /\ WorkflowTask_added_store_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "stored"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowTask_added_terminate_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "added"

WorkflowTask_added_terminate_AnyHosting(entity) ==
    /\ WorkflowTask_added_terminate_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowTask_created_terminate_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \notin exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "created"

WorkflowTask_created_terminate_AnyHosting(entity) ==
    /\ WorkflowTask_created_terminate_AnyHostingEnabled(entity)
    /\ exists_WorkflowTask' = exists_WorkflowTask \union {entity}
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowTask_stored_discard_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "stored"

WorkflowTask_stored_discard_AnyHosting(entity) ==
    /\ WorkflowTask_stored_discard_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "discarded"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

WorkflowTask_stored_terminate_AnyHostingEnabled(entity) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ state_WorkflowTask[entity] = "stored"

WorkflowTask_stored_terminate_AnyHosting(entity) ==
    /\ WorkflowTask_stored_terminate_AnyHostingEnabled(entity)
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Delivery_acknowledgeEnabled(task, attempt) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \in exists_DeliveryAttempt
    /\ (state_DeliveryTask[task] = "dispatched" /\ state_DeliveryAttempt[attempt] = "dispatched" /\ <<attempt, task>> \in relation_delivery_attempt_task)

Delivery_acknowledge(task, attempt) ==
    /\ Delivery_acknowledgeEnabled(task, attempt)
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "completed"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "acknowledged"]
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Delivery_dispatchEnabled(task, attempt) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \in exists_DeliveryAttempt
    /\ (state_DeliveryTask[task] = "authorized" /\ state_DeliveryAttempt[attempt] = "accepted" /\ <<attempt, task>> \in relation_delivery_attempt_task)

Delivery_dispatch(task, attempt) ==
    /\ Delivery_dispatchEnabled(task, attempt)
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "dispatched"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "dispatched"]
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Delivery_expireEnabled(obligation, task) ==
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ (state_WorkObligation[obligation] = "valid" /\ (state_DeliveryTask[task] = "pending" \/ state_DeliveryTask[task] = "sync-offered" \/ state_DeliveryTask[task] = "backlogged") /\ <<task, obligation>> \in relation_delivery_task_obligation)

Delivery_expire(obligation, task) ==
    /\ Delivery_expireEnabled(obligation, task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "retired"]
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "terminal"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Delivery_offer_syncEnabled(task) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ (state_DeliveryTask[task] = "pending" /\ (\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ state_WorkObligation[obligation] = "valid"))))

Delivery_offer_sync(task) ==
    /\ Delivery_offer_syncEnabled(task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "sync-offered"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Delivery_persist_ambiguousEnabled(obligation, task, queue) ==
    /\ obligation \in WorkObligationIDs
    /\ obligation \notin exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \notin exists_DeliveryTask
    /\ queue \in DeliveryQueueIDs
    /\ queue \in exists_DeliveryQueue

Delivery_persist_ambiguous(obligation, task, queue) ==
    /\ Delivery_persist_ambiguousEnabled(obligation, task, queue)
    /\ exists_DeliveryTask' = exists_DeliveryTask \union {task}
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "pending"]
    /\ exists_WorkObligation' = exists_WorkObligation \union {obligation}
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "unresolved"]
    /\ relation_delivery_task_obligation' = (relation_delivery_task_obligation) \union {<<task, obligation>>}
    /\ relation_delivery_task_queue' = (relation_delivery_task_queue) \union {<<task, queue>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_Poller, state_Poller, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Delivery_reserveEnabled(task, attempt, poller) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \notin exists_DeliveryAttempt
    /\ poller \in PollerIDs
    /\ poller \in exists_Poller
    /\ (state_DeliveryTask[task] = "sync-offered" \/ state_DeliveryTask[task] = "backlogged")

Delivery_reserve(task, attempt, poller) ==
    /\ Delivery_reserveEnabled(task, attempt, poller)
    /\ exists_DeliveryAttempt' = exists_DeliveryAttempt \union {attempt}
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "reserved"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "reserved"]
    /\ relation_delivery_attempt_poller' = (relation_delivery_attempt_poller) \union {<<attempt, poller>>}
    /\ relation_delivery_attempt_task' = (relation_delivery_attempt_task) \union {<<attempt, task>>}
    /\ UNCHANGED <<exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Delivery_retireEnabled(task) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ state_DeliveryTask[task] = "acknowledged"

Delivery_retire(task) ==
    /\ Delivery_retireEnabled(task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "retired"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Delivery_retryEnabled(task, attempt) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \in exists_DeliveryAttempt
    /\ (state_DeliveryTask[task] = "dispatched" /\ state_DeliveryAttempt[attempt] = "dispatched" /\ <<attempt, task>> \in relation_delivery_attempt_task)

Delivery_retry(task, attempt) ==
    /\ Delivery_retryEnabled(task, attempt)
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "failed"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "backlogged"]
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Delivery_spoolEnabled(task) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ ((state_DeliveryTask[task] = "pending" \/ state_DeliveryTask[task] = "sync-offered") /\ (\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ state_WorkObligation[obligation] = "valid"))))

Delivery_spool(task) ==
    /\ Delivery_spoolEnabled(task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "backlogged"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_delivery_authorize_addedEnabled(entity, obligation, task, attempt) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \in exists_DeliveryAttempt
    /\ (state_WorkflowTask[entity] = "added" /\ <<entity, obligation>> \in relation_workflow_task_obligation /\ <<entity, task>> \in relation_workflow_task_delivery_task /\ (state_WorkObligation[obligation] = "valid" /\ state_DeliveryTask[task] = "reserved" /\ state_DeliveryAttempt[attempt] = "reserved" /\ <<task, obligation>> \in relation_delivery_task_obligation /\ <<attempt, task>> \in relation_delivery_attempt_task))

Workflow_delivery_authorize_added(entity, obligation, task, attempt) ==
    /\ Workflow_delivery_authorize_addedEnabled(entity, obligation, task, attempt)
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "accepted"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "authorized"]
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "accepted"]
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "polled"]
    /\ relation_delivery_accepted_start' = (relation_delivery_accepted_start) \union {<<obligation, attempt>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_delivery_authorize_storedEnabled(entity, obligation, task, attempt) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \in exists_DeliveryAttempt
    /\ (state_WorkflowTask[entity] = "stored" /\ <<entity, obligation>> \in relation_workflow_task_obligation /\ <<entity, task>> \in relation_workflow_task_delivery_task /\ (state_WorkObligation[obligation] = "valid" /\ state_DeliveryTask[task] = "reserved" /\ state_DeliveryAttempt[attempt] = "reserved" /\ <<task, obligation>> \in relation_delivery_task_obligation /\ <<attempt, task>> \in relation_delivery_attempt_task))

Workflow_delivery_authorize_stored(entity, obligation, task, attempt) ==
    /\ Workflow_delivery_authorize_storedEnabled(entity, obligation, task, attempt)
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "accepted"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "authorized"]
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "accepted"]
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "polled"]
    /\ relation_delivery_accepted_start' = (relation_delivery_accepted_start) \union {<<obligation, attempt>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_delivery_persistEnabled(entity, obligation, task, queue) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \notin exists_WorkflowTask
    /\ obligation \in WorkObligationIDs
    /\ obligation \notin exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \notin exists_DeliveryTask
    /\ queue \in DeliveryQueueIDs
    /\ queue \in exists_DeliveryQueue
    /\ state_WorkflowTask[entity] = "created"

Workflow_delivery_persist(entity, obligation, task, queue) ==
    /\ Workflow_delivery_persistEnabled(entity, obligation, task, queue)
    /\ exists_DeliveryTask' = exists_DeliveryTask \union {task}
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "pending"]
    /\ exists_WorkObligation' = exists_WorkObligation \union {obligation}
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "valid"]
    /\ exists_WorkflowTask' = exists_WorkflowTask \union {entity}
    /\ state_WorkflowTask' = [state_WorkflowTask EXCEPT ![entity] = "added"]
    /\ relation_delivery_task_obligation' = (relation_delivery_task_obligation) \union {<<task, obligation>>}
    /\ relation_delivery_task_queue' = (relation_delivery_task_queue) \union {<<task, queue>>}
    /\ relation_workflow_task_delivery_task' = (relation_workflow_task_delivery_task) \union {<<entity, task>>}
    /\ relation_workflow_task_obligation' = (relation_workflow_task_obligation) \union {<<entity, obligation>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_Poller, state_Poller, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_workflow_run_successor, relation_workflow_runs>>

Workflow_delivery_reject_addedEnabled(entity, obligation, task, attempt) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \in exists_DeliveryAttempt
    /\ (state_WorkflowTask[entity] = "added" /\ <<entity, obligation>> \in relation_workflow_task_obligation /\ <<entity, task>> \in relation_workflow_task_delivery_task /\ (state_WorkObligation[obligation] = "valid" /\ state_DeliveryTask[task] = "reserved" /\ state_DeliveryAttempt[attempt] = "reserved" /\ <<task, obligation>> \in relation_delivery_task_obligation /\ <<attempt, task>> \in relation_delivery_attempt_task))

Workflow_delivery_reject_added(entity, obligation, task, attempt) ==
    /\ Workflow_delivery_reject_addedEnabled(entity, obligation, task, attempt)
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "rejected"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "backlogged"]
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_delivery_reject_storedEnabled(entity, obligation, task, attempt) ==
    /\ entity \in WorkflowTaskIDs
    /\ entity \in exists_WorkflowTask
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \in exists_DeliveryAttempt
    /\ (state_WorkflowTask[entity] = "stored" /\ <<entity, obligation>> \in relation_workflow_task_obligation /\ <<entity, task>> \in relation_workflow_task_delivery_task /\ (state_WorkObligation[obligation] = "valid" /\ state_DeliveryTask[task] = "reserved" /\ state_DeliveryAttempt[attempt] = "reserved" /\ <<task, obligation>> \in relation_delivery_task_obligation /\ <<attempt, task>> \in relation_delivery_attempt_task))

Workflow_delivery_reject_stored(entity, obligation, task, attempt) ==
    /\ Workflow_delivery_reject_storedEnabled(entity, obligation, task, attempt)
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "rejected"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "backlogged"]
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Workflow_delivery_resolve_persistedEnabled(obligation, task) ==
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ (state_WorkObligation[obligation] = "unresolved" /\ state_DeliveryTask[task] = "pending" /\ <<task, obligation>> \in relation_delivery_task_obligation)

Workflow_delivery_resolve_persisted(obligation, task) ==
    /\ Workflow_delivery_resolve_persistedEnabled(obligation, task)
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "valid"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, exists_Workflow, state_Workflow, exists_WorkflowRun, state_WorkflowRun, exists_WorkflowTask, state_WorkflowTask, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue, relation_workflow_run_successor, relation_workflow_runs, relation_workflow_task_delivery_task, relation_workflow_task_obligation>>

Next ==
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
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_store_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_terminate_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_created_terminate_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_discard_AnyHosting(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_terminate_AnyHosting(entity)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_acknowledge(task, attempt)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_dispatch(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_expire(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_offer_sync(task)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_ambiguous(obligation, task, queue)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs, poller \in PollerIDs: Delivery_reserve(task, attempt, poller)
    \/ \E task \in DeliveryTaskIDs: Delivery_retire(task)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_retry(task, attempt)
    \/ \E task \in DeliveryTaskIDs: Delivery_spool(task)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Workflow_delivery_authorize_added(entity, obligation, task, attempt)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Workflow_delivery_authorize_stored(entity, obligation, task, attempt)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Workflow_delivery_persist(entity, obligation, task, queue)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Workflow_delivery_reject_added(entity, obligation, task, attempt)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Workflow_delivery_reject_stored(entity, obligation, task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Workflow_delivery_resolve_persisted(obligation, task)

CanStep ==
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
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_store_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_added_terminate_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_created_terminate_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_discard_AnyHostingEnabled(entity)
    \/ \E entity \in WorkflowTaskIDs: WorkflowTask_stored_terminate_AnyHostingEnabled(entity)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_acknowledgeEnabled(task, attempt)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_dispatchEnabled(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_expireEnabled(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_offer_syncEnabled(task)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_ambiguousEnabled(obligation, task, queue)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs, poller \in PollerIDs: Delivery_reserveEnabled(task, attempt, poller)
    \/ \E task \in DeliveryTaskIDs: Delivery_retireEnabled(task)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_retryEnabled(task, attempt)
    \/ \E task \in DeliveryTaskIDs: Delivery_spoolEnabled(task)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Workflow_delivery_authorize_addedEnabled(entity, obligation, task, attempt)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Workflow_delivery_authorize_storedEnabled(entity, obligation, task, attempt)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Workflow_delivery_persistEnabled(entity, obligation, task, queue)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Workflow_delivery_reject_addedEnabled(entity, obligation, task, attempt)
    \/ \E entity \in WorkflowTaskIDs, obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Workflow_delivery_reject_storedEnabled(entity, obligation, task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Workflow_delivery_resolve_persistedEnabled(obligation, task)

Workflow_started_quiescent_progress ==
    (\A entity \in WorkflowIDs: entity \in exists_Workflow => (~(state_Workflow[entity] = "started")))

delivery_ambiguous_commit_resolved ==
    (\A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => (~(state_WorkObligation[obligation] = "unresolved")))

delivery_coarse_retirement_safety ==
    (\A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => ((state_DeliveryTask[task] = "retired" => (\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ (state_WorkObligation[obligation] = "accepted" \/ state_WorkObligation[obligation] = "terminal")))))))

delivery_destination_isolation ==
    (\A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => ((\E queue \in DeliveryQueueIDs: queue \in exists_DeliveryQueue /\ (<<task, queue>> \in relation_delivery_task_queue))))

delivery_failed_start_is_not_accepted ==
    (\A attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt => ((state_DeliveryAttempt[attempt] = "rejected" => (\A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => (~(<<obligation, attempt>> \in relation_delivery_accepted_start))))))

delivery_no_phantom_dispatch ==
    (\A attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt => (((state_DeliveryAttempt[attempt] = "dispatched" \/ state_DeliveryAttempt[attempt] = "failed" \/ state_DeliveryAttempt[attempt] = "completed") => (\E task \in DeliveryTaskIDs: task \in exists_DeliveryTask /\ ((<<attempt, task>> \in relation_delivery_attempt_task /\ (\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ state_WorkObligation[obligation] = "accepted")))))))))

delivery_no_resurrection ==
    (\A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => ((state_WorkObligation[obligation] = "terminal" => (\A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => ((<<task, obligation>> \in relation_delivery_task_obligation => state_DeliveryTask[task] = "retired"))))))

delivery_no_split_commit ==
    (\A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => (((state_WorkObligation[obligation] = "valid" \/ state_WorkObligation[obligation] = "accepted") => (\E task \in DeliveryTaskIDs: task \in exists_DeliveryTask /\ (<<task, obligation>> \in relation_delivery_task_obligation)))))

delivery_path_equivalence ==
    (\A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => (((\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ (<<task, obligation>> \in relation_delivery_task_obligation)) /\ (\E queue \in DeliveryQueueIDs: queue \in exists_DeliveryQueue /\ (<<task, queue>> \in relation_delivery_task_queue)))))

delivery_retry_preserves_obligation ==
    (\A attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt => ((\E task \in DeliveryTaskIDs: task \in exists_DeliveryTask /\ ((<<attempt, task>> \in relation_delivery_attempt_task /\ (\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ (<<task, obligation>> \in relation_delivery_task_obligation)))))))

delivery_single_accepted_start ==
    (\A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => ((state_WorkObligation[obligation] = "accepted" => (\E attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt /\ (<<obligation, attempt>> \in relation_delivery_accepted_start)))))

workflow_delivery_accepted_start_correspondence ==
    (\A workflowTask \in WorkflowTaskIDs: workflowTask \in exists_WorkflowTask => ((state_WorkflowTask[workflowTask] = "polled" => (\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<workflowTask, obligation>> \in relation_workflow_task_obligation /\ state_WorkObligation[obligation] = "accepted" /\ (\E attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt /\ (<<obligation, attempt>> \in relation_delivery_accepted_start))))))))

workflow_delivery_intent_correspondence ==
    (\A workflowTask \in WorkflowTaskIDs: workflowTask \in exists_WorkflowTask => (((state_WorkflowTask[workflowTask] = "added" \/ state_WorkflowTask[workflowTask] = "stored" \/ state_WorkflowTask[workflowTask] = "polled") => ((\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ (<<workflowTask, obligation>> \in relation_workflow_task_obligation)) /\ (\E deliveryTask \in DeliveryTaskIDs: deliveryTask \in exists_DeliveryTask /\ (<<workflowTask, deliveryTask>> \in relation_workflow_task_delivery_task))))))

InductiveInvariant ==
    /\ TypeOK
    /\ Cardinality_delivery_accepted_start
    /\ Cardinality_delivery_attempt_poller
    /\ Cardinality_delivery_attempt_task
    /\ Cardinality_delivery_task_obligation
    /\ Cardinality_delivery_task_queue
    /\ Cardinality_workflow_run_successor
    /\ Cardinality_workflow_runs
    /\ Cardinality_workflow_task_delivery_task
    /\ Cardinality_workflow_task_obligation
    /\ delivery_coarse_retirement_safety
    /\ delivery_destination_isolation
    /\ delivery_failed_start_is_not_accepted
    /\ delivery_no_phantom_dispatch
    /\ delivery_no_resurrection
    /\ delivery_no_split_commit
    /\ delivery_path_equivalence
    /\ delivery_retry_preserves_obligation
    /\ delivery_single_accepted_start
    /\ workflow_delivery_accepted_start_correspondence
    /\ workflow_delivery_intent_correspondence
DeclaredSafety ==
    /\ delivery_coarse_retirement_safety
    /\ delivery_destination_isolation
    /\ delivery_failed_start_is_not_accepted
    /\ delivery_no_phantom_dispatch
    /\ delivery_no_resurrection
    /\ delivery_no_split_commit
    /\ delivery_path_equivalence
    /\ delivery_retry_preserves_obligation
    /\ delivery_single_accepted_start
    /\ workflow_delivery_accepted_start_correspondence
    /\ workflow_delivery_intent_correspondence
Safety == InductiveInvariant /\ DeclaredSafety

QuiescentSafety ==
    \/ CanStep
    \/ /\ Workflow_started_quiescent_progress
       /\ delivery_ambiguous_commit_resolved

Spec == Init /\ [][Next]_vars

====
