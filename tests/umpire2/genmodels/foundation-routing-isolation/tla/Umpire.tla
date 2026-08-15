---- MODULE Umpire ----
EXTENDS FiniteSets, Naturals

CONSTANTS
    \* @type: Set(Str);
    DeliveryAttemptIDs,
    \* @type: Set(Str);
    DeliveryQueueIDs,
    \* @type: Set(Str);
    DeliveryRouteClassIDs,
    \* @type: Set(Str);
    DeliveryTaskIDs,
    \* @type: Set(Str);
    HistoryOwnerGenerationIDs,
    \* @type: Set(Str);
    HistoryShardIDs,
    \* @type: Set(Str);
    MatchingOwnerGenerationIDs,
    \* @type: Set(Str);
    MatchingQueuePartitionIDs,
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
    exists_DeliveryRouteClass,
    \* @type: Str -> Str;
    state_DeliveryRouteClass,
    \* @type: Set(Str);
    exists_DeliveryTask,
    \* @type: Str -> Str;
    state_DeliveryTask,
    \* @type: Set(Str);
    exists_HistoryOwnerGeneration,
    \* @type: Str -> Str;
    state_HistoryOwnerGeneration,
    \* @type: Set(Str);
    exists_HistoryShard,
    \* @type: Str -> Str;
    state_HistoryShard,
    \* @type: Set(Str);
    exists_MatchingOwnerGeneration,
    \* @type: Str -> Str;
    state_MatchingOwnerGeneration,
    \* @type: Set(Str);
    exists_MatchingQueuePartition,
    \* @type: Str -> Str;
    state_MatchingQueuePartition,
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
    relation_delivery_partition_owner,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_partition_route,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_poller_route,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_history_owner_generation,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_history_shard,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_obligation,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_owner_generation,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_queue,
    \* @type: Set(<<Str, Str>>);
    relation_delivery_task_route,
    \* @type: Set(<<Str, Str>>);
    relation_history_shard_owner

vars == <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, state_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

TypeOK ==
    /\ exists_DeliveryAttempt \in SUBSET DeliveryAttemptIDs
    /\ state_DeliveryAttempt \in [DeliveryAttemptIDs -> {"accepted", "completed", "dispatched", "failed", "rejected", "reserved"}]
    /\ exists_DeliveryQueue \in SUBSET DeliveryQueueIDs
    /\ state_DeliveryQueue \in [DeliveryQueueIDs -> {"available"}]
    /\ exists_DeliveryRouteClass \in SUBSET DeliveryRouteClassIDs
    /\ state_DeliveryRouteClass \in [DeliveryRouteClassIDs -> {"active", "inactive"}]
    /\ exists_DeliveryTask \in SUBSET DeliveryTaskIDs
    /\ state_DeliveryTask \in [DeliveryTaskIDs -> {"acknowledged", "authorized", "backlogged", "dispatched", "pending", "reserved", "retired", "sync-offered"}]
    /\ exists_HistoryOwnerGeneration \in SUBSET HistoryOwnerGenerationIDs
    /\ state_HistoryOwnerGeneration \in [HistoryOwnerGenerationIDs -> {"current", "stale", "unused"}]
    /\ exists_HistoryShard \in SUBSET HistoryShardIDs
    /\ state_HistoryShard \in [HistoryShardIDs -> {"owned", "unowned"}]
    /\ exists_MatchingOwnerGeneration \in SUBSET MatchingOwnerGenerationIDs
    /\ state_MatchingOwnerGeneration \in [MatchingOwnerGenerationIDs -> {"current", "stale", "unused"}]
    /\ exists_MatchingQueuePartition \in SUBSET MatchingQueuePartitionIDs
    /\ state_MatchingQueuePartition \in [MatchingQueuePartitionIDs -> {"owned", "unowned"}]
    /\ exists_Poller \in SUBSET PollerIDs
    /\ state_Poller \in [PollerIDs -> {"available"}]
    /\ exists_WorkObligation \in SUBSET WorkObligationIDs
    /\ state_WorkObligation \in [WorkObligationIDs -> {"accepted", "terminal", "unresolved", "valid"}]
    /\ relation_delivery_accepted_start \in SUBSET (WorkObligationIDs \X DeliveryAttemptIDs)
    /\ relation_delivery_attempt_poller \in SUBSET (DeliveryAttemptIDs \X PollerIDs)
    /\ relation_delivery_attempt_task \in SUBSET (DeliveryAttemptIDs \X DeliveryTaskIDs)
    /\ relation_delivery_partition_owner \in SUBSET (MatchingQueuePartitionIDs \X MatchingOwnerGenerationIDs)
    /\ relation_delivery_partition_route \in SUBSET (MatchingQueuePartitionIDs \X DeliveryRouteClassIDs)
    /\ relation_delivery_poller_route \in SUBSET (PollerIDs \X DeliveryRouteClassIDs)
    /\ relation_delivery_task_history_owner_generation \in SUBSET (DeliveryTaskIDs \X HistoryOwnerGenerationIDs)
    /\ relation_delivery_task_history_shard \in SUBSET (DeliveryTaskIDs \X HistoryShardIDs)
    /\ relation_delivery_task_obligation \in SUBSET (DeliveryTaskIDs \X WorkObligationIDs)
    /\ relation_delivery_task_owner_generation \in SUBSET (DeliveryTaskIDs \X MatchingOwnerGenerationIDs)
    /\ relation_delivery_task_queue \in SUBSET (DeliveryTaskIDs \X DeliveryQueueIDs)
    /\ relation_delivery_task_route \in SUBSET (DeliveryTaskIDs \X DeliveryRouteClassIDs)
    /\ relation_history_shard_owner \in SUBSET (HistoryShardIDs \X HistoryOwnerGenerationIDs)

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

Cardinality_delivery_partition_owner ==
    /\ \A tuple \in relation_delivery_partition_owner: tuple[1] \in exists_MatchingQueuePartition /\ tuple[2] \in exists_MatchingOwnerGeneration
    /\ \A source \in MatchingQueuePartitionIDs: Cardinality({target \in MatchingOwnerGenerationIDs: <<source, target>> \in relation_delivery_partition_owner}) <= 1

Cardinality_delivery_partition_route ==
    /\ \A tuple \in relation_delivery_partition_route: tuple[1] \in exists_MatchingQueuePartition /\ tuple[2] \in exists_DeliveryRouteClass
    /\ \A source \in MatchingQueuePartitionIDs: Cardinality({target \in DeliveryRouteClassIDs: <<source, target>> \in relation_delivery_partition_route}) <= 1

Cardinality_delivery_poller_route ==
    /\ \A tuple \in relation_delivery_poller_route: tuple[1] \in exists_Poller /\ tuple[2] \in exists_DeliveryRouteClass
    /\ \A source \in PollerIDs: Cardinality({target \in DeliveryRouteClassIDs: <<source, target>> \in relation_delivery_poller_route}) <= 1

Cardinality_delivery_task_history_owner_generation ==
    /\ \A tuple \in relation_delivery_task_history_owner_generation: tuple[1] \in exists_DeliveryTask /\ tuple[2] \in exists_HistoryOwnerGeneration
    /\ \A source \in DeliveryTaskIDs: Cardinality({target \in HistoryOwnerGenerationIDs: <<source, target>> \in relation_delivery_task_history_owner_generation}) <= 1

Cardinality_delivery_task_history_shard ==
    /\ \A tuple \in relation_delivery_task_history_shard: tuple[1] \in exists_DeliveryTask /\ tuple[2] \in exists_HistoryShard
    /\ \A source \in DeliveryTaskIDs: Cardinality({target \in HistoryShardIDs: <<source, target>> \in relation_delivery_task_history_shard}) <= 1

Cardinality_delivery_task_obligation ==
    /\ \A tuple \in relation_delivery_task_obligation: tuple[1] \in exists_DeliveryTask /\ tuple[2] \in exists_WorkObligation
    /\ \A source \in DeliveryTaskIDs: Cardinality({target \in WorkObligationIDs: <<source, target>> \in relation_delivery_task_obligation}) <= 1

Cardinality_delivery_task_owner_generation ==
    /\ \A tuple \in relation_delivery_task_owner_generation: tuple[1] \in exists_DeliveryTask /\ tuple[2] \in exists_MatchingOwnerGeneration
    /\ \A source \in DeliveryTaskIDs: Cardinality({target \in MatchingOwnerGenerationIDs: <<source, target>> \in relation_delivery_task_owner_generation}) <= 1

Cardinality_delivery_task_queue ==
    /\ \A tuple \in relation_delivery_task_queue: tuple[1] \in exists_DeliveryTask /\ tuple[2] \in exists_DeliveryQueue
    /\ \A source \in DeliveryTaskIDs: Cardinality({target \in DeliveryQueueIDs: <<source, target>> \in relation_delivery_task_queue}) <= 1

Cardinality_delivery_task_route ==
    /\ \A tuple \in relation_delivery_task_route: tuple[1] \in exists_DeliveryTask /\ tuple[2] \in exists_DeliveryRouteClass
    /\ \A source \in DeliveryTaskIDs: Cardinality({target \in DeliveryRouteClassIDs: <<source, target>> \in relation_delivery_task_route}) <= 1

Cardinality_history_shard_owner ==
    /\ \A tuple \in relation_history_shard_owner: tuple[1] \in exists_HistoryShard /\ tuple[2] \in exists_HistoryOwnerGeneration
    /\ \A source \in HistoryShardIDs: Cardinality({target \in HistoryOwnerGenerationIDs: <<source, target>> \in relation_history_shard_owner}) <= 1

Init ==
    /\ exists_DeliveryAttempt = {}
    /\ state_DeliveryAttempt = [entity \in DeliveryAttemptIDs |-> "reserved"]
    /\ exists_DeliveryQueue = {"DeliveryQueue#0", "DeliveryQueue#1"}
    /\ state_DeliveryQueue = [entity \in DeliveryQueueIDs |-> "available"]
    /\ exists_DeliveryRouteClass = {}
    /\ state_DeliveryRouteClass = [entity \in DeliveryRouteClassIDs |-> "inactive"]
    /\ exists_DeliveryTask = {}
    /\ state_DeliveryTask = [entity \in DeliveryTaskIDs |-> "pending"]
    /\ exists_HistoryOwnerGeneration = {}
    /\ state_HistoryOwnerGeneration = [entity \in HistoryOwnerGenerationIDs |-> "unused"]
    /\ exists_HistoryShard = {}
    /\ state_HistoryShard = [entity \in HistoryShardIDs |-> "unowned"]
    /\ exists_MatchingOwnerGeneration = {}
    /\ state_MatchingOwnerGeneration = [entity \in MatchingOwnerGenerationIDs |-> "unused"]
    /\ exists_MatchingQueuePartition = {}
    /\ state_MatchingQueuePartition = [entity \in MatchingQueuePartitionIDs |-> "unowned"]
    /\ exists_Poller = {"Poller#0", "Poller#1"}
    /\ state_Poller = [entity \in PollerIDs |-> "available"]
    /\ exists_WorkObligation = {}
    /\ state_WorkObligation = [entity \in WorkObligationIDs |-> "unresolved"]
    /\ relation_delivery_accepted_start = {}
    /\ relation_delivery_attempt_poller = {}
    /\ relation_delivery_attempt_task = {}
    /\ relation_delivery_partition_owner = {}
    /\ relation_delivery_partition_route = {}
    /\ relation_delivery_poller_route = {}
    /\ relation_delivery_task_history_owner_generation = {}
    /\ relation_delivery_task_history_shard = {}
    /\ relation_delivery_task_obligation = {}
    /\ relation_delivery_task_owner_generation = {}
    /\ relation_delivery_task_queue = {}
    /\ relation_delivery_task_route = {}
    /\ relation_history_shard_owner = {}

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
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

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
    /\ relation_delivery_accepted_start' = relation_delivery_accepted_start \union {<<obligation, attempt>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

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
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

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
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

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
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

Delivery_offer_syncEnabled(task) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ (state_DeliveryTask[task] = "pending" /\ \E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ state_WorkObligation[obligation] = "valid")))

Delivery_offer_sync(task) ==
    /\ Delivery_offer_syncEnabled(task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "sync-offered"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

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
    /\ relation_delivery_task_obligation' = relation_delivery_task_obligation \union {<<task, obligation>>}
    /\ relation_delivery_task_queue' = relation_delivery_task_queue \union {<<task, queue>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_owner_generation, relation_delivery_task_route, relation_history_shard_owner>>

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
    /\ relation_delivery_task_obligation' = relation_delivery_task_obligation \union {<<task, obligation>>}
    /\ relation_delivery_task_queue' = relation_delivery_task_queue \union {<<task, queue>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_owner_generation, relation_delivery_task_route, relation_history_shard_owner>>

Delivery_resolve_persistedEnabled(obligation, task) ==
    /\ obligation \in WorkObligationIDs
    /\ obligation \in exists_WorkObligation
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ (state_WorkObligation[obligation] = "unresolved" /\ state_DeliveryTask[task] = "pending" /\ <<task, obligation>> \in relation_delivery_task_obligation)

Delivery_resolve_persisted(obligation, task) ==
    /\ Delivery_resolve_persistedEnabled(obligation, task)
    /\ state_WorkObligation' = [state_WorkObligation EXCEPT ![obligation] = "valid"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, state_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

Delivery_retireEnabled(task) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ state_DeliveryTask[task] = "acknowledged"

Delivery_retire(task) ==
    /\ Delivery_retireEnabled(task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "retired"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

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
    /\ UNCHANGED <<exists_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

Delivery_spoolEnabled(task) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ ((state_DeliveryTask[task] = "pending" \/ state_DeliveryTask[task] = "sync-offered") /\ \E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ state_WorkObligation[obligation] = "valid")))

Delivery_spool(task) ==
    /\ Delivery_spoolEnabled(task)
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "backlogged"]
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

Routing_bootstrapEnabled(route, partition, generation) ==
    /\ route \in DeliveryRouteClassIDs
    /\ route \notin exists_DeliveryRouteClass
    /\ partition \in MatchingQueuePartitionIDs
    /\ partition \notin exists_MatchingQueuePartition
    /\ generation \in MatchingOwnerGenerationIDs
    /\ generation \notin exists_MatchingOwnerGeneration

Routing_bootstrap(route, partition, generation) ==
    /\ Routing_bootstrapEnabled(route, partition, generation)
    /\ exists_DeliveryRouteClass' = exists_DeliveryRouteClass \union {route}
    /\ state_DeliveryRouteClass' = [state_DeliveryRouteClass EXCEPT ![route] = "active"]
    /\ exists_MatchingOwnerGeneration' = exists_MatchingOwnerGeneration \union {generation}
    /\ state_MatchingOwnerGeneration' = [state_MatchingOwnerGeneration EXCEPT ![generation] = "current"]
    /\ exists_MatchingQueuePartition' = exists_MatchingQueuePartition \union {partition}
    /\ state_MatchingQueuePartition' = [state_MatchingQueuePartition EXCEPT ![partition] = "owned"]
    /\ relation_delivery_partition_owner' = relation_delivery_partition_owner \union {<<partition, generation>>}
    /\ relation_delivery_partition_route' = relation_delivery_partition_route \union {<<partition, route>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryTask, state_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

Routing_bootstrap_history_ownerEnabled(shard, historyGeneration) ==
    /\ shard \in HistoryShardIDs
    /\ shard \notin exists_HistoryShard
    /\ historyGeneration \in HistoryOwnerGenerationIDs
    /\ historyGeneration \notin exists_HistoryOwnerGeneration

Routing_bootstrap_history_owner(shard, historyGeneration) ==
    /\ Routing_bootstrap_history_ownerEnabled(shard, historyGeneration)
    /\ exists_HistoryOwnerGeneration' = exists_HistoryOwnerGeneration \union {historyGeneration}
    /\ state_HistoryOwnerGeneration' = [state_HistoryOwnerGeneration EXCEPT ![historyGeneration] = "current"]
    /\ exists_HistoryShard' = exists_HistoryShard \union {shard}
    /\ state_HistoryShard' = [state_HistoryShard EXCEPT ![shard] = "owned"]
    /\ relation_history_shard_owner' = relation_history_shard_owner \union {<<shard, historyGeneration>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, state_DeliveryTask, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route>>

Routing_forward_to_matchingEnabled(task, route, partition, generation, shard, historyGeneration) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ route \in DeliveryRouteClassIDs
    /\ route \in exists_DeliveryRouteClass
    /\ partition \in MatchingQueuePartitionIDs
    /\ partition \in exists_MatchingQueuePartition
    /\ generation \in MatchingOwnerGenerationIDs
    /\ generation \in exists_MatchingOwnerGeneration
    /\ shard \in HistoryShardIDs
    /\ shard \in exists_HistoryShard
    /\ historyGeneration \in HistoryOwnerGenerationIDs
    /\ historyGeneration \in exists_HistoryOwnerGeneration
    /\ ((state_DeliveryTask[task] = "pending" \/ state_DeliveryTask[task] = "sync-offered" \/ state_DeliveryTask[task] = "backlogged") /\ state_MatchingOwnerGeneration[generation] = "current" /\ <<partition, route>> \in relation_delivery_partition_route /\ <<partition, generation>> \in relation_delivery_partition_owner /\ state_HistoryOwnerGeneration[historyGeneration] = "current" /\ <<shard, historyGeneration>> \in relation_history_shard_owner)

Routing_forward_to_matching(task, route, partition, generation, shard, historyGeneration) ==
    /\ Routing_forward_to_matchingEnabled(task, route, partition, generation, shard, historyGeneration)
    /\ relation_delivery_task_history_owner_generation' = relation_delivery_task_history_owner_generation \union {<<task, historyGeneration>>}
    /\ relation_delivery_task_history_shard' = relation_delivery_task_history_shard \union {<<task, shard>>}
    /\ relation_delivery_task_owner_generation' = relation_delivery_task_owner_generation \union {<<task, generation>>}
    /\ relation_delivery_task_route' = relation_delivery_task_route \union {<<task, route>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, state_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_obligation, relation_delivery_task_queue, relation_history_shard_owner>>

Routing_handoffEnabled(partition, oldGeneration, newGeneration) ==
    /\ partition \in MatchingQueuePartitionIDs
    /\ partition \in exists_MatchingQueuePartition
    /\ oldGeneration \in MatchingOwnerGenerationIDs
    /\ oldGeneration \in exists_MatchingOwnerGeneration
    /\ newGeneration \in MatchingOwnerGenerationIDs
    /\ newGeneration \notin exists_MatchingOwnerGeneration
    /\ (state_MatchingOwnerGeneration[oldGeneration] = "current" /\ <<partition, oldGeneration>> \in relation_delivery_partition_owner /\ \A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => ((<<task, oldGeneration>> \in relation_delivery_task_owner_generation => (state_DeliveryTask[task] = "acknowledged" \/ state_DeliveryTask[task] = "retired"))))

Routing_handoff(partition, oldGeneration, newGeneration) ==
    /\ Routing_handoffEnabled(partition, oldGeneration, newGeneration)
    /\ exists_MatchingOwnerGeneration' = exists_MatchingOwnerGeneration \union {newGeneration}
    /\ state_MatchingOwnerGeneration' = [state_MatchingOwnerGeneration EXCEPT ![oldGeneration] = "stale", ![newGeneration] = "current"]
    /\ relation_delivery_partition_owner' = (relation_delivery_partition_owner) \ {<<partition, oldGeneration>>} \union {<<partition, newGeneration>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, state_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

Routing_handoff_history_ownerEnabled(shard, oldHistoryGeneration, newHistoryGeneration) ==
    /\ shard \in HistoryShardIDs
    /\ shard \in exists_HistoryShard
    /\ oldHistoryGeneration \in HistoryOwnerGenerationIDs
    /\ oldHistoryGeneration \in exists_HistoryOwnerGeneration
    /\ newHistoryGeneration \in HistoryOwnerGenerationIDs
    /\ newHistoryGeneration \notin exists_HistoryOwnerGeneration
    /\ (state_HistoryOwnerGeneration[oldHistoryGeneration] = "current" /\ <<shard, oldHistoryGeneration>> \in relation_history_shard_owner /\ \A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => ((<<task, oldHistoryGeneration>> \in relation_delivery_task_history_owner_generation => (state_DeliveryTask[task] = "acknowledged" \/ state_DeliveryTask[task] = "retired"))))

Routing_handoff_history_owner(shard, oldHistoryGeneration, newHistoryGeneration) ==
    /\ Routing_handoff_history_ownerEnabled(shard, oldHistoryGeneration, newHistoryGeneration)
    /\ exists_HistoryOwnerGeneration' = exists_HistoryOwnerGeneration \union {newHistoryGeneration}
    /\ state_HistoryOwnerGeneration' = [state_HistoryOwnerGeneration EXCEPT ![oldHistoryGeneration] = "stale", ![newHistoryGeneration] = "current"]
    /\ relation_history_shard_owner' = (relation_history_shard_owner) \ {<<shard, oldHistoryGeneration>>} \union {<<shard, newHistoryGeneration>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, state_DeliveryTask, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route>>

Routing_register_pollerEnabled(poller, route) ==
    /\ poller \in PollerIDs
    /\ poller \in exists_Poller
    /\ route \in DeliveryRouteClassIDs
    /\ route \in exists_DeliveryRouteClass

Routing_register_poller(poller, route) ==
    /\ Routing_register_pollerEnabled(poller, route)
    /\ relation_delivery_poller_route' = relation_delivery_poller_route \union {<<poller, route>>}
    /\ UNCHANGED <<exists_DeliveryAttempt, state_DeliveryAttempt, exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, state_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_attempt_poller, relation_delivery_attempt_task, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

Routing_reserve_compatibleEnabled(task, attempt, poller, route, partition, generation) ==
    /\ task \in DeliveryTaskIDs
    /\ task \in exists_DeliveryTask
    /\ attempt \in DeliveryAttemptIDs
    /\ attempt \notin exists_DeliveryAttempt
    /\ poller \in PollerIDs
    /\ poller \in exists_Poller
    /\ route \in DeliveryRouteClassIDs
    /\ route \in exists_DeliveryRouteClass
    /\ partition \in MatchingQueuePartitionIDs
    /\ partition \in exists_MatchingQueuePartition
    /\ generation \in MatchingOwnerGenerationIDs
    /\ generation \in exists_MatchingOwnerGeneration
    /\ ((state_DeliveryTask[task] = "sync-offered" \/ state_DeliveryTask[task] = "backlogged") /\ <<task, route>> \in relation_delivery_task_route /\ <<poller, route>> \in relation_delivery_poller_route /\ <<partition, route>> \in relation_delivery_partition_route /\ <<partition, generation>> \in relation_delivery_partition_owner /\ <<task, generation>> \in relation_delivery_task_owner_generation /\ state_MatchingOwnerGeneration[generation] = "current")

Routing_reserve_compatible(task, attempt, poller, route, partition, generation) ==
    /\ Routing_reserve_compatibleEnabled(task, attempt, poller, route, partition, generation)
    /\ exists_DeliveryAttempt' = exists_DeliveryAttempt \union {attempt}
    /\ state_DeliveryAttempt' = [state_DeliveryAttempt EXCEPT ![attempt] = "reserved"]
    /\ state_DeliveryTask' = [state_DeliveryTask EXCEPT ![task] = "reserved"]
    /\ relation_delivery_attempt_poller' = relation_delivery_attempt_poller \union {<<attempt, poller>>}
    /\ relation_delivery_attempt_task' = relation_delivery_attempt_task \union {<<attempt, task>>}
    /\ UNCHANGED <<exists_DeliveryQueue, state_DeliveryQueue, exists_DeliveryRouteClass, state_DeliveryRouteClass, exists_DeliveryTask, exists_HistoryOwnerGeneration, state_HistoryOwnerGeneration, exists_HistoryShard, state_HistoryShard, exists_MatchingOwnerGeneration, state_MatchingOwnerGeneration, exists_MatchingQueuePartition, state_MatchingQueuePartition, exists_Poller, state_Poller, exists_WorkObligation, state_WorkObligation, relation_delivery_accepted_start, relation_delivery_partition_owner, relation_delivery_partition_route, relation_delivery_poller_route, relation_delivery_task_history_owner_generation, relation_delivery_task_history_shard, relation_delivery_task_obligation, relation_delivery_task_owner_generation, relation_delivery_task_queue, relation_delivery_task_route, relation_history_shard_owner>>

Next ==
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_acknowledge(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_authorize_accept(obligation, task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_authorize_reject(obligation, task, attempt)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_dispatch(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_expire(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_offer_sync(task)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_ambiguous(obligation, task, queue)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_success(obligation, task, queue)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_resolve_persisted(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_retire(task)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_retry(task, attempt)
    \/ \E task \in DeliveryTaskIDs: Delivery_spool(task)
    \/ \E route \in DeliveryRouteClassIDs, partition \in MatchingQueuePartitionIDs, generation \in MatchingOwnerGenerationIDs: Routing_bootstrap(route, partition, generation)
    \/ \E shard \in HistoryShardIDs, historyGeneration \in HistoryOwnerGenerationIDs: Routing_bootstrap_history_owner(shard, historyGeneration)
    \/ \E task \in DeliveryTaskIDs, route \in DeliveryRouteClassIDs, partition \in MatchingQueuePartitionIDs, generation \in MatchingOwnerGenerationIDs, shard \in HistoryShardIDs, historyGeneration \in HistoryOwnerGenerationIDs: Routing_forward_to_matching(task, route, partition, generation, shard, historyGeneration)
    \/ \E partition \in MatchingQueuePartitionIDs, oldGeneration \in MatchingOwnerGenerationIDs, newGeneration \in MatchingOwnerGenerationIDs: Routing_handoff(partition, oldGeneration, newGeneration)
    \/ \E shard \in HistoryShardIDs, oldHistoryGeneration \in HistoryOwnerGenerationIDs, newHistoryGeneration \in HistoryOwnerGenerationIDs: Routing_handoff_history_owner(shard, oldHistoryGeneration, newHistoryGeneration)
    \/ \E poller \in PollerIDs, route \in DeliveryRouteClassIDs: Routing_register_poller(poller, route)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs, poller \in PollerIDs, route \in DeliveryRouteClassIDs, partition \in MatchingQueuePartitionIDs, generation \in MatchingOwnerGenerationIDs: Routing_reserve_compatible(task, attempt, poller, route, partition, generation)

CanStep ==
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_acknowledgeEnabled(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_authorize_acceptEnabled(obligation, task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_authorize_rejectEnabled(obligation, task, attempt)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_dispatchEnabled(task, attempt)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_expireEnabled(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_offer_syncEnabled(task)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_ambiguousEnabled(obligation, task, queue)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs, queue \in DeliveryQueueIDs: Delivery_persist_successEnabled(obligation, task, queue)
    \/ \E obligation \in WorkObligationIDs, task \in DeliveryTaskIDs: Delivery_resolve_persistedEnabled(obligation, task)
    \/ \E task \in DeliveryTaskIDs: Delivery_retireEnabled(task)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs: Delivery_retryEnabled(task, attempt)
    \/ \E task \in DeliveryTaskIDs: Delivery_spoolEnabled(task)
    \/ \E route \in DeliveryRouteClassIDs, partition \in MatchingQueuePartitionIDs, generation \in MatchingOwnerGenerationIDs: Routing_bootstrapEnabled(route, partition, generation)
    \/ \E shard \in HistoryShardIDs, historyGeneration \in HistoryOwnerGenerationIDs: Routing_bootstrap_history_ownerEnabled(shard, historyGeneration)
    \/ \E task \in DeliveryTaskIDs, route \in DeliveryRouteClassIDs, partition \in MatchingQueuePartitionIDs, generation \in MatchingOwnerGenerationIDs, shard \in HistoryShardIDs, historyGeneration \in HistoryOwnerGenerationIDs: Routing_forward_to_matchingEnabled(task, route, partition, generation, shard, historyGeneration)
    \/ \E partition \in MatchingQueuePartitionIDs, oldGeneration \in MatchingOwnerGenerationIDs, newGeneration \in MatchingOwnerGenerationIDs: Routing_handoffEnabled(partition, oldGeneration, newGeneration)
    \/ \E shard \in HistoryShardIDs, oldHistoryGeneration \in HistoryOwnerGenerationIDs, newHistoryGeneration \in HistoryOwnerGenerationIDs: Routing_handoff_history_ownerEnabled(shard, oldHistoryGeneration, newHistoryGeneration)
    \/ \E poller \in PollerIDs, route \in DeliveryRouteClassIDs: Routing_register_pollerEnabled(poller, route)
    \/ \E task \in DeliveryTaskIDs, attempt \in DeliveryAttemptIDs, poller \in PollerIDs, route \in DeliveryRouteClassIDs, partition \in MatchingQueuePartitionIDs, generation \in MatchingOwnerGenerationIDs: Routing_reserve_compatibleEnabled(task, attempt, poller, route, partition, generation)

delivery_ambiguous_commit_resolved ==
    \A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => (~(state_WorkObligation[obligation] = "unresolved"))

delivery_coarse_retirement_safety ==
    \A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => ((state_DeliveryTask[task] = "retired" => \E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ (state_WorkObligation[obligation] = "accepted" \/ state_WorkObligation[obligation] = "terminal")))))

delivery_destination_isolation ==
    \A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => (\E queue \in DeliveryQueueIDs: queue \in exists_DeliveryQueue /\ (<<task, queue>> \in relation_delivery_task_queue))

delivery_failed_start_is_not_accepted ==
    \A attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt => ((state_DeliveryAttempt[attempt] = "rejected" => \A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => (~(<<obligation, attempt>> \in relation_delivery_accepted_start))))

delivery_no_phantom_dispatch ==
    \A attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt => (((state_DeliveryAttempt[attempt] = "dispatched" \/ state_DeliveryAttempt[attempt] = "failed" \/ state_DeliveryAttempt[attempt] = "completed") => \E task \in DeliveryTaskIDs: task \in exists_DeliveryTask /\ ((<<attempt, task>> \in relation_delivery_attempt_task /\ \E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ ((<<task, obligation>> \in relation_delivery_task_obligation /\ state_WorkObligation[obligation] = "accepted"))))))

delivery_no_resurrection ==
    \A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => ((state_WorkObligation[obligation] = "terminal" => \A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => ((<<task, obligation>> \in relation_delivery_task_obligation => state_DeliveryTask[task] = "retired"))))

delivery_no_split_commit ==
    \A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => (((state_WorkObligation[obligation] = "valid" \/ state_WorkObligation[obligation] = "accepted") => \E task \in DeliveryTaskIDs: task \in exists_DeliveryTask /\ (<<task, obligation>> \in relation_delivery_task_obligation)))

delivery_owner_generation_fencing ==
    \A attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt => (((state_DeliveryAttempt[attempt] = "reserved" \/ state_DeliveryAttempt[attempt] = "accepted" \/ state_DeliveryAttempt[attempt] = "dispatched") => \E task \in DeliveryTaskIDs: task \in exists_DeliveryTask /\ ((<<attempt, task>> \in relation_delivery_attempt_task /\ \E generation \in MatchingOwnerGenerationIDs: generation \in exists_MatchingOwnerGeneration /\ ((<<task, generation>> \in relation_delivery_task_owner_generation /\ state_MatchingOwnerGeneration[generation] = "current")) /\ \E historyGeneration \in HistoryOwnerGenerationIDs: historyGeneration \in exists_HistoryOwnerGeneration /\ ((<<task, historyGeneration>> \in relation_delivery_task_history_owner_generation /\ state_HistoryOwnerGeneration[historyGeneration] = "current"))))))

delivery_path_equivalence ==
    \A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => ((\E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ (<<task, obligation>> \in relation_delivery_task_obligation) /\ \E queue \in DeliveryQueueIDs: queue \in exists_DeliveryQueue /\ (<<task, queue>> \in relation_delivery_task_queue)))

delivery_retry_preserves_obligation ==
    \A attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt => (\E task \in DeliveryTaskIDs: task \in exists_DeliveryTask /\ ((<<attempt, task>> \in relation_delivery_attempt_task /\ \E obligation \in WorkObligationIDs: obligation \in exists_WorkObligation /\ (<<task, obligation>> \in relation_delivery_task_obligation))))

delivery_routing_isolation ==
    \A attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt => (\A task \in DeliveryTaskIDs: task \in exists_DeliveryTask => (\A poller \in PollerIDs: poller \in exists_Poller => (((<<attempt, task>> \in relation_delivery_attempt_task /\ <<attempt, poller>> \in relation_delivery_attempt_poller) => \E route \in DeliveryRouteClassIDs: route \in exists_DeliveryRouteClass /\ ((<<task, route>> \in relation_delivery_task_route /\ <<poller, route>> \in relation_delivery_poller_route))))))

delivery_single_accepted_start ==
    \A obligation \in WorkObligationIDs: obligation \in exists_WorkObligation => ((state_WorkObligation[obligation] = "accepted" => \E attempt \in DeliveryAttemptIDs: attempt \in exists_DeliveryAttempt /\ (<<obligation, attempt>> \in relation_delivery_accepted_start)))

InductiveInvariant ==
    /\ TypeOK
    /\ Cardinality_delivery_accepted_start
    /\ Cardinality_delivery_attempt_poller
    /\ Cardinality_delivery_attempt_task
    /\ Cardinality_delivery_partition_owner
    /\ Cardinality_delivery_partition_route
    /\ Cardinality_delivery_poller_route
    /\ Cardinality_delivery_task_history_owner_generation
    /\ Cardinality_delivery_task_history_shard
    /\ Cardinality_delivery_task_obligation
    /\ Cardinality_delivery_task_owner_generation
    /\ Cardinality_delivery_task_queue
    /\ Cardinality_delivery_task_route
    /\ Cardinality_history_shard_owner
    /\ delivery_coarse_retirement_safety
    /\ delivery_destination_isolation
    /\ delivery_failed_start_is_not_accepted
    /\ delivery_no_phantom_dispatch
    /\ delivery_no_resurrection
    /\ delivery_no_split_commit
    /\ delivery_owner_generation_fencing
    /\ delivery_path_equivalence
    /\ delivery_retry_preserves_obligation
    /\ delivery_routing_isolation
    /\ delivery_single_accepted_start
DeclaredSafety ==
    /\ delivery_coarse_retirement_safety
    /\ delivery_destination_isolation
    /\ delivery_failed_start_is_not_accepted
    /\ delivery_no_phantom_dispatch
    /\ delivery_no_resurrection
    /\ delivery_no_split_commit
    /\ delivery_owner_generation_fencing
    /\ delivery_path_equivalence
    /\ delivery_retry_preserves_obligation
    /\ delivery_routing_isolation
    /\ delivery_single_accepted_start
Safety == InductiveInvariant /\ DeclaredSafety

QuiescentSafety == CanStep \/ delivery_ambiguous_commit_resolved

Spec == Init /\ [][Next]_vars

====
