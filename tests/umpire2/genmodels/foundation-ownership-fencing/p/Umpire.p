// Generated from the Umpire verification snapshot. Do not edit.

event eStep;

enum DeliveryAttempt { DeliveryAttempt_0 }
enum DeliveryAttempt_state { DeliveryAttempt_state_accepted, DeliveryAttempt_state_completed, DeliveryAttempt_state_dispatched, DeliveryAttempt_state_failed, DeliveryAttempt_state_rejected, DeliveryAttempt_state_reserved }
enum DeliveryQueue { DeliveryQueue_0, DeliveryQueue_1 }
enum DeliveryQueue_state { DeliveryQueue_state_available }
enum DeliveryRouteClass { DeliveryRouteClass_0 }
enum DeliveryRouteClass_state { DeliveryRouteClass_state_active, DeliveryRouteClass_state_inactive }
enum DeliveryTask { DeliveryTask_0 }
enum DeliveryTask_state { DeliveryTask_state_acknowledged, DeliveryTask_state_authorized, DeliveryTask_state_backlogged, DeliveryTask_state_dispatched, DeliveryTask_state_pending, DeliveryTask_state_reserved, DeliveryTask_state_retired, DeliveryTask_state_sync_offered }
enum HistoryOwnerGeneration { HistoryOwnerGeneration_0, HistoryOwnerGeneration_1 }
enum HistoryOwnerGeneration_state { HistoryOwnerGeneration_state_current, HistoryOwnerGeneration_state_stale, HistoryOwnerGeneration_state_unused }
enum HistoryShard { HistoryShard_0 }
enum HistoryShard_state { HistoryShard_state_owned, HistoryShard_state_unowned }
enum MatchingOwnerGeneration { MatchingOwnerGeneration_0, MatchingOwnerGeneration_1 }
enum MatchingOwnerGeneration_state { MatchingOwnerGeneration_state_current, MatchingOwnerGeneration_state_stale, MatchingOwnerGeneration_state_unused }
enum MatchingQueuePartition { MatchingQueuePartition_0 }
enum MatchingQueuePartition_state { MatchingQueuePartition_state_owned, MatchingQueuePartition_state_unowned }
enum Poller { Poller_0, Poller_1 }
enum Poller_state { Poller_state_available }
enum WorkObligation { WorkObligation_0 }
enum WorkObligation_state { WorkObligation_state_accepted, WorkObligation_state_terminal, WorkObligation_state_unresolved, WorkObligation_state_valid }
type relation_delivery_accepted_start_tuple = (source: WorkObligation, target: DeliveryAttempt);
type relation_delivery_attempt_poller_tuple = (source: DeliveryAttempt, target: Poller);
type relation_delivery_attempt_task_tuple = (source: DeliveryAttempt, target: DeliveryTask);
type relation_delivery_partition_owner_tuple = (source: MatchingQueuePartition, target: MatchingOwnerGeneration);
type relation_delivery_partition_route_tuple = (source: MatchingQueuePartition, target: DeliveryRouteClass);
type relation_delivery_poller_route_tuple = (source: Poller, target: DeliveryRouteClass);
type relation_delivery_task_history_owner_generation_tuple = (source: DeliveryTask, target: HistoryOwnerGeneration);
type relation_delivery_task_history_shard_tuple = (source: DeliveryTask, target: HistoryShard);
type relation_delivery_task_obligation_tuple = (source: DeliveryTask, target: WorkObligation);
type relation_delivery_task_owner_generation_tuple = (source: DeliveryTask, target: MatchingOwnerGeneration);
type relation_delivery_task_queue_tuple = (source: DeliveryTask, target: DeliveryQueue);
type relation_delivery_task_route_tuple = (source: DeliveryTask, target: DeliveryRouteClass);
type relation_history_shard_owner_tuple = (source: HistoryShard, target: HistoryOwnerGeneration);

machine UmpireWorld {
  var checkerStep: int;
  var exists_DeliveryAttempt: set[DeliveryAttempt];
  var state_DeliveryAttempt: map[DeliveryAttempt, DeliveryAttempt_state];
  var exists_DeliveryQueue: set[DeliveryQueue];
  var state_DeliveryQueue: map[DeliveryQueue, DeliveryQueue_state];
  var exists_DeliveryRouteClass: set[DeliveryRouteClass];
  var state_DeliveryRouteClass: map[DeliveryRouteClass, DeliveryRouteClass_state];
  var exists_DeliveryTask: set[DeliveryTask];
  var state_DeliveryTask: map[DeliveryTask, DeliveryTask_state];
  var exists_HistoryOwnerGeneration: set[HistoryOwnerGeneration];
  var state_HistoryOwnerGeneration: map[HistoryOwnerGeneration, HistoryOwnerGeneration_state];
  var exists_HistoryShard: set[HistoryShard];
  var state_HistoryShard: map[HistoryShard, HistoryShard_state];
  var exists_MatchingOwnerGeneration: set[MatchingOwnerGeneration];
  var state_MatchingOwnerGeneration: map[MatchingOwnerGeneration, MatchingOwnerGeneration_state];
  var exists_MatchingQueuePartition: set[MatchingQueuePartition];
  var state_MatchingQueuePartition: map[MatchingQueuePartition, MatchingQueuePartition_state];
  var exists_Poller: set[Poller];
  var state_Poller: map[Poller, Poller_state];
  var exists_WorkObligation: set[WorkObligation];
  var state_WorkObligation: map[WorkObligation, WorkObligation_state];
  var relation_delivery_accepted_start: set[relation_delivery_accepted_start_tuple];
  var relation_delivery_attempt_poller: set[relation_delivery_attempt_poller_tuple];
  var relation_delivery_attempt_task: set[relation_delivery_attempt_task_tuple];
  var relation_delivery_partition_owner: set[relation_delivery_partition_owner_tuple];
  var relation_delivery_partition_route: set[relation_delivery_partition_route_tuple];
  var relation_delivery_poller_route: set[relation_delivery_poller_route_tuple];
  var relation_delivery_task_history_owner_generation: set[relation_delivery_task_history_owner_generation_tuple];
  var relation_delivery_task_history_shard: set[relation_delivery_task_history_shard_tuple];
  var relation_delivery_task_obligation: set[relation_delivery_task_obligation_tuple];
  var relation_delivery_task_owner_generation: set[relation_delivery_task_owner_generation_tuple];
  var relation_delivery_task_queue: set[relation_delivery_task_queue_tuple];
  var relation_delivery_task_route: set[relation_delivery_task_route_tuple];
  var relation_history_shard_owner: set[relation_history_shard_owner_tuple];

  start state Init {
    entry {
      state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
      state_DeliveryQueue[DeliveryQueue_0] = DeliveryQueue_state_available;
      state_DeliveryQueue[DeliveryQueue_1] = DeliveryQueue_state_available;
      exists_DeliveryQueue += (DeliveryQueue_0);
      exists_DeliveryQueue += (DeliveryQueue_1);
      state_DeliveryRouteClass[DeliveryRouteClass_0] = DeliveryRouteClass_state_inactive;
      state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
      state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_unused;
      state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_unused;
      state_HistoryShard[HistoryShard_0] = HistoryShard_state_unowned;
      state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_unused;
      state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_unused;
      state_MatchingQueuePartition[MatchingQueuePartition_0] = MatchingQueuePartition_state_unowned;
      state_Poller[Poller_0] = Poller_state_available;
      state_Poller[Poller_1] = Poller_state_available;
      exists_Poller += (Poller_0);
      exists_Poller += (Poller_1);
      state_WorkObligation[WorkObligation_0] = WorkObligation_state_unresolved;
      CheckSafety();
      send this, eStep;
    }
    on eStep do Step;
  }

  fun Step() {
    var enabled: set[int];
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (0); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (1); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (2); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (3); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (4); }
    if (DeliveryTask_0 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid)))))) { enabled += (5); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (6); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (7); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (8); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (9); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (10); }
    if (DeliveryTask_0 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged) { enabled += (11); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (12); }
    if (DeliveryTask_0 in exists_DeliveryTask && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid)))))) { enabled += (13); }
    if (!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_0 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration)) { enabled += (14); }
    if (!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_0 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration)) { enabled += (15); }
    if (!(HistoryShard_0 in exists_HistoryShard) && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration)) { enabled += (16); }
    if (!(HistoryShard_0 in exists_HistoryShard) && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration)) { enabled += (17); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (18); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (19); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (20); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (21); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (22); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (23); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (24); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (25); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (26); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (27); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (28); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (29); }
    if (Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass) { enabled += (30); }
    if (Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass) { enabled += (31); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (32); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (33); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (34); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (35); }
    if (sizeof(enabled) == 0) {
      CheckQuiescent();
      raise halt;
    }
    if (0 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (0);
    }
    if (1 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (1);
    }
    if (2 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (2);
    }
    if (3 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (3);
    }
    if (4 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (4);
    }
    if (5 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      enabled -= (5);
    }
    if (6 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (6);
    }
    if (7 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (7);
    }
    if (8 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (8);
    }
    if (9 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (9);
    }
    if (10 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (10);
    }
    if (11 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retire_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_retire_DeliveryTask_0(); return; }
      enabled -= (11);
    }
    if (12 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (12);
    }
    if (13 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_spool_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_spool_DeliveryTask_0(); return; }
      enabled -= (13);
    }
    if (14 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (14);
    }
    if (15 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (15);
    }
    if (16 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (16);
    }
    if (17 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (17);
    }
    if (18 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (18);
    }
    if (19 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (19);
    }
    if (20 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (20);
    }
    if (21 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (21);
    }
    if (22 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (22);
    }
    if (23 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (23);
    }
    if (24 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (24);
    }
    if (25 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (25);
    }
    if (26 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (26);
    }
    if (27 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (27);
    }
    if (28 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (28);
    }
    if (29 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (29);
    }
    if (30 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_register_poller_Poller_0_DeliveryRouteClass_0(); return; }
      if ($) { Apply_routing_register_poller_Poller_0_DeliveryRouteClass_0(); return; }
      enabled -= (30);
    }
    if (31 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_register_poller_Poller_1_DeliveryRouteClass_0(); return; }
      if ($) { Apply_routing_register_poller_Poller_1_DeliveryRouteClass_0(); return; }
      enabled -= (31);
    }
    if (32 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (32);
    }
    if (33 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (33);
    }
    if (34 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (34);
    }
    if (35 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (35);
    }
  }

  fun Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.acknowledge task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_acknowledged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_completed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.authorize.accept obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.authorize.reject obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.dispatch task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_dispatched;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_dispatched;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_expire_WorkObligation_0_DeliveryTask_0() {
    print "UMPIRE_ACTION delivery.expire obligation=WorkObligation#0 task=DeliveryTask#0";
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_offer_sync_DeliveryTask_0() {
    print "UMPIRE_ACTION delivery.offer-sync task=DeliveryTask#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_sync_offered;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION delivery.persist.ambiguous obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_unresolved;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION delivery.persist.ambiguous obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_unresolved;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION delivery.persist.success obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION delivery.persist.success obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0() {
    print "UMPIRE_ACTION delivery.resolve-persisted obligation=WorkObligation#0 task=DeliveryTask#0";
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_retire_DeliveryTask_0() {
    print "UMPIRE_ACTION delivery.retire task=DeliveryTask#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.retry task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_spool_DeliveryTask_0() {
    print "UMPIRE_ACTION delivery.spool task=DeliveryTask#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.bootstrap route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    exists_DeliveryRouteClass += (DeliveryRouteClass_0);
    state_DeliveryRouteClass[DeliveryRouteClass_0] = DeliveryRouteClass_state_active;
    exists_MatchingQueuePartition += (MatchingQueuePartition_0);
    state_MatchingQueuePartition[MatchingQueuePartition_0] = MatchingQueuePartition_state_owned;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_0);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_route += ((source = MatchingQueuePartition_0, target = DeliveryRouteClass_0));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.bootstrap route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    exists_DeliveryRouteClass += (DeliveryRouteClass_0);
    state_DeliveryRouteClass[DeliveryRouteClass_0] = DeliveryRouteClass_state_active;
    exists_MatchingQueuePartition += (MatchingQueuePartition_0);
    state_MatchingQueuePartition[MatchingQueuePartition_0] = MatchingQueuePartition_state_owned;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_1);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_route += ((source = MatchingQueuePartition_0, target = DeliveryRouteClass_0));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.bootstrap-history-owner shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    exists_HistoryShard += (HistoryShard_0);
    state_HistoryShard[HistoryShard_0] = HistoryShard_state_owned;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_0);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner += ((source = HistoryShard_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.bootstrap-history-owner shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    exists_HistoryShard += (HistoryShard_0);
    state_HistoryShard[HistoryShard_0] = HistoryShard_state_owned;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_1);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner += ((source = HistoryShard_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.handoff partition=MatchingQueuePartition#0 oldGeneration=MatchingOwnerGeneration#0 newGeneration=MatchingOwnerGeneration#0";
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_stale;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_0);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_owner -= ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.handoff partition=MatchingQueuePartition#0 oldGeneration=MatchingOwnerGeneration#0 newGeneration=MatchingOwnerGeneration#1";
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_stale;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_1);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_owner -= ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.handoff partition=MatchingQueuePartition#0 oldGeneration=MatchingOwnerGeneration#1 newGeneration=MatchingOwnerGeneration#0";
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_stale;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_0);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_owner -= ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.handoff partition=MatchingQueuePartition#0 oldGeneration=MatchingOwnerGeneration#1 newGeneration=MatchingOwnerGeneration#1";
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_stale;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_1);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_owner -= ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.handoff-history-owner shard=HistoryShard#0 oldHistoryGeneration=HistoryOwnerGeneration#0 newHistoryGeneration=HistoryOwnerGeneration#0";
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_stale;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_0);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner -= ((source = HistoryShard_0, target = HistoryOwnerGeneration_0));
    relation_history_shard_owner += ((source = HistoryShard_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.handoff-history-owner shard=HistoryShard#0 oldHistoryGeneration=HistoryOwnerGeneration#0 newHistoryGeneration=HistoryOwnerGeneration#1";
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_stale;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_1);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner -= ((source = HistoryShard_0, target = HistoryOwnerGeneration_0));
    relation_history_shard_owner += ((source = HistoryShard_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.handoff-history-owner shard=HistoryShard#0 oldHistoryGeneration=HistoryOwnerGeneration#1 newHistoryGeneration=HistoryOwnerGeneration#0";
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_stale;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_0);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner -= ((source = HistoryShard_0, target = HistoryOwnerGeneration_1));
    relation_history_shard_owner += ((source = HistoryShard_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.handoff-history-owner shard=HistoryShard#0 oldHistoryGeneration=HistoryOwnerGeneration#1 newHistoryGeneration=HistoryOwnerGeneration#1";
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_stale;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_1);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner -= ((source = HistoryShard_0, target = HistoryOwnerGeneration_1));
    relation_history_shard_owner += ((source = HistoryShard_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_register_poller_Poller_0_DeliveryRouteClass_0() {
    print "UMPIRE_ACTION routing.register-poller poller=Poller#0 route=DeliveryRouteClass#0";
    relation_delivery_poller_route += ((source = Poller_0, target = DeliveryRouteClass_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_register_poller_Poller_1_DeliveryRouteClass_0() {
    print "UMPIRE_ACTION routing.register-poller poller=Poller#1 route=DeliveryRouteClass#0";
    relation_delivery_poller_route += ((source = Poller_1, target = DeliveryRouteClass_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun CheckSafety() {
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start) || (WorkObligation_0 in exists_WorkObligation && DeliveryAttempt_0 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_0 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_1 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller && (source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller), "relation delivery-attempt-poller exceeds source cardinality";
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task) || (DeliveryAttempt_0 in exists_DeliveryAttempt && DeliveryTask_0 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner), "relation delivery-partition-owner exceeds source cardinality";
    assert !((source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
    assert !((source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route) || (Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route) || (Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && (source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation), "relation delivery-task-history-owner-generation exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = HistoryShard_0) in relation_delivery_task_history_shard) || (DeliveryTask_0 in exists_DeliveryTask && HistoryShard_0 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation), "relation delivery-task-owner-generation exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner) || (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner) || (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner), "relation history-shard-owner exceeds source cardinality";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))))))))), "property delivery.coarse-retirement-safety failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue)))))), "property delivery.destination-isolation failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start))))))))), "property delivery.failed-start-is-not-accepted failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))))))))))))), "property delivery.no-phantom-dispatch failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))))), "property delivery.no-resurrection failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)))))))), "property delivery.no-split-commit failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current))))))))))))), "property delivery.owner-generation-fencing failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue))))))), "property delivery.path-equivalence failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)))))))))), "property delivery.retry-preserves-obligation failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)))))))), "property delivery.single-accepted-start failed";
  }

  fun CheckQuiescent() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved)))), "quiescent property delivery.ambiguous-commit-resolved failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
