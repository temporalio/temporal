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
    WorkObligationIDs

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
    \* @type: Set(<<Str, Str>>);
    relation_delivery_accepted_start,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_attempt_poller,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_attempt_task,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_obligation,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_queue

vars == <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

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
    /\ relation_delivery_accepted_start \in SUBSET (WorkObligationIDs \X DeliveryAttemptIDs)
    /\ relation_delivery_attempt_poller \in SUBSET (DeliveryAttemptIDs \X PollerIDs)
    /\ relation_delivery_attempt_task \in SUBSET (DeliveryAttemptIDs \X DeliveryTaskIDs)
    /\ relation_delivery_task_obligation \in SUBSET (DeliveryTaskIDs \X WorkObligationIDs)
    /\ relation_delivery_task_queue \in SUBSET (DeliveryTaskIDs \X DeliveryQueueIDs)

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
    /\ relation_delivery_accepted_start = {}
    /\ relation_delivery_attempt_poller = {}
    /\ relation_delivery_attempt_task = {}
    /\ relation_delivery_task_obligation = {}
    /\ relation_delivery_task_queue = {}

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
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

Delivery_authorize_acceptEnabled(obligation, task, attempt) ==
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \in exists_DeliveryAttempt
    /\ (state_WorkObligation[obligation] = "valid" /\ state_DeliveryTask[task] = "reserved" /\ state_DeliveryAttempt[attempt] = "reserved" /\ <<task, obligation>> \in relation_delivery_task_obligation /\ <<attempt, task>> \in relation_delivery_attempt_task)

Delivery_authorize_accept(obligation, task, attempt) ==
    /\ Delivery_authorize_acceptEnabled(obligation, task, attempt)
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "accepted"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "authorized"]
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "accepted"]
    /\ relation_delivery_accepted_start' = (relation_delivery_accepted_start) \union {<<obligation, attempt>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

Delivery_authorize_rejectEnabled(obligation, task, attempt) ==
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \in exists_DeliveryAttempt
    /\ (state_WorkObligation[obligation] = "valid" /\ state_DeliveryTask[task] = "reserved" /\ state_DeliveryAttempt[attempt] = "reserved" /\ <<task, obligation>> \in relation_delivery_task_obligation /\ <<attempt, task>> \in relation_delivery_attempt_task)

Delivery_authorize_reject(obligation, task, attempt) ==
    /\ Delivery_authorize_rejectEnabled(obligation, task, attempt)
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "rejected"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "backlogged"]
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

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
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

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
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

Delivery_offer_syncEnabled(task) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ (state_DeliveryTask[task] = "pending" /\ (\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ state_WorkObligation[obligation] = "valid"))))

Delivery_offer_sync(task) ==
    /\ Delivery_offer_syncEnabled(task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "sync-offered"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

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
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_Poller, state_Poller, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task>>

Delivery_persist_successEnabled(obligation, task, queue) ==
    /\ obligation \in WorkObligationIDs
    /\ obligation \notin exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \notin exists_DeliveryTask
    /\ queue \in DeliveryQueueIDs
    /\ queue \in exists_DeliveryQueue

Delivery_persist_success(obligation, task, queue) ==
    /\ Delivery_persist_successEnabled(obligation, task, queue)
    /\ exists_DeliveryTask' = exists_DeliveryTask \union {task}
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "pending"]
    /\ exists_WorkObligation' = exists_WorkObligation \union {obligation}
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "valid"]
    /\ relation_delivery_task_obligation' = (relation_delivery_task_obligation) \union {<<task, obligation>>}
    /\ relation_delivery_task_queue' = (relation_delivery_task_queue) \union {<<task, queue>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_Poller, state_Poller, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task>>

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
    /\ UNCHANGED <<exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_task_obligation, relation_delivery_task_queue>>

Delivery_resolve_persistedEnabled(obligation, task) ==
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ (state_WorkObligation[obligation] = "unresolved" /\ state_DeliveryTask[task] = "pending" /\ <<task, obligation>> \in relation_delivery_task_obligation)

Delivery_resolve_persisted(obligation, task) ==
    /\ Delivery_resolve_persistedEnabled(obligation, task)
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "valid"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

Delivery_retireEnabled(task) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ state_DeliveryTask[task] = "acknowledged"

Delivery_retire(task) ==
    /\ Delivery_retireEnabled(task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "retired"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

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
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

Delivery_spoolEnabled(task) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ ((state_DeliveryTask[task] = "pending" \/ state_DeliveryTask[task] = "sync-offered") /\ (\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ state_WorkObligation[obligation] = "valid"))))

Delivery_spool(task) ==
    /\ Delivery_spoolEnabled(task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "backlogged"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_task_obligation, relation_delivery_task_queue>>

Next ==
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_acknowledge(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_authorize_accept(obligation, task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_authorize_reject(obligation, task, attempt)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_dispatch(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_expire(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_offer_sync(task)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_ambiguous(obligation, task, queue)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_success(obligation, task, queue)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs, poller \in PollerIDs: Delivery_reserve(task, attempt, poller)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_resolve_persisted(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_retire(task)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_retry(task, attempt)
    \/ \E task \in DeliveryTaskIDs: Delivery_spool(task)

CanStep ==
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_acknowledgeEnabled(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_authorize_acceptEnabled(obligation, task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_authorize_rejectEnabled(obligation, task, attempt)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_dispatchEnabled(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_expireEnabled(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_offer_syncEnabled(task)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_ambiguousEnabled(obligation, task, queue)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_successEnabled(obligation, task, queue)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs, poller \in PollerIDs: Delivery_reserveEnabled(task, attempt, poller)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_resolve_persistedEnabled(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_retireEnabled(task)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_retryEnabled(task, attempt)
    \/ \E task \in DeliveryTaskIDs: Delivery_spoolEnabled(task)

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

InductiveInvariant ==
    /\ TypeOK
    /\ Cardinality_delivery_accepted_start
    /\ Cardinality_delivery_attempt_poller
    /\ Cardinality_delivery_attempt_task
    /\ Cardinality_delivery_task_obligation
    /\ Cardinality_delivery_task_queue
    /\ delivery_coarse_retirement_safety
    /\ delivery_destination_isolation
    /\ delivery_failed_start_is_not_accepted
    /\ delivery_no_phantom_dispatch
    /\ delivery_no_resurrection
    /\ delivery_no_split_commit
    /\ delivery_path_equivalence
    /\ delivery_retry_preserves_obligation
    /\ delivery_single_accepted_start
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
Safety == InductiveInvariant /\ DeclaredSafety

QuiescentSafety == CanStep \/ delivery_ambiguous_commit_resolved

Spec == Init /\ [][Next]_vars

====
