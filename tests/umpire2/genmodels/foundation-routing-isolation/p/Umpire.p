// Generated from the Umpire verification snapshot. Do not edit.

event eStep;

enum DeliveryAttempt { DeliveryAttempt_0, DeliveryAttempt_1 }
enum DeliveryAttempt_state { DeliveryAttempt_state_accepted, DeliveryAttempt_state_completed, DeliveryAttempt_state_dispatched, DeliveryAttempt_state_failed, DeliveryAttempt_state_rejected, DeliveryAttempt_state_reserved }
enum DeliveryQueue { DeliveryQueue_0, DeliveryQueue_1 }
enum DeliveryQueue_state { DeliveryQueue_state_available }
enum DeliveryRouteClass { DeliveryRouteClass_0, DeliveryRouteClass_1 }
enum DeliveryRouteClass_state { DeliveryRouteClass_state_active, DeliveryRouteClass_state_inactive }
enum DeliveryTask { DeliveryTask_0, DeliveryTask_1 }
enum DeliveryTask_state { DeliveryTask_state_acknowledged, DeliveryTask_state_authorized, DeliveryTask_state_backlogged, DeliveryTask_state_dispatched, DeliveryTask_state_pending, DeliveryTask_state_reserved, DeliveryTask_state_retired, DeliveryTask_state_sync_offered }
enum HistoryOwnerGeneration { HistoryOwnerGeneration_0, HistoryOwnerGeneration_1 }
enum HistoryOwnerGeneration_state { HistoryOwnerGeneration_state_current, HistoryOwnerGeneration_state_stale, HistoryOwnerGeneration_state_unused }
enum HistoryShard { HistoryShard_0, HistoryShard_1 }
enum HistoryShard_state { HistoryShard_state_owned, HistoryShard_state_unowned }
enum MatchingOwnerGeneration { MatchingOwnerGeneration_0, MatchingOwnerGeneration_1 }
enum MatchingOwnerGeneration_state { MatchingOwnerGeneration_state_current, MatchingOwnerGeneration_state_stale, MatchingOwnerGeneration_state_unused }
enum MatchingQueuePartition { MatchingQueuePartition_0, MatchingQueuePartition_1 }
enum MatchingQueuePartition_state { MatchingQueuePartition_state_owned, MatchingQueuePartition_state_unowned }
enum Poller { Poller_0, Poller_1 }
enum Poller_state { Poller_state_available }
enum WorkObligation { WorkObligation_0, WorkObligation_1 }
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
      state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
      state_DeliveryQueue[DeliveryQueue_0] = DeliveryQueue_state_available;
      state_DeliveryQueue[DeliveryQueue_1] = DeliveryQueue_state_available;
      exists_DeliveryQueue += (DeliveryQueue_0);
      exists_DeliveryQueue += (DeliveryQueue_1);
      state_DeliveryRouteClass[DeliveryRouteClass_0] = DeliveryRouteClass_state_inactive;
      state_DeliveryRouteClass[DeliveryRouteClass_1] = DeliveryRouteClass_state_inactive;
      state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
      state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
      state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_unused;
      state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_unused;
      state_HistoryShard[HistoryShard_0] = HistoryShard_state_unowned;
      state_HistoryShard[HistoryShard_1] = HistoryShard_state_unowned;
      state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_unused;
      state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_unused;
      state_MatchingQueuePartition[MatchingQueuePartition_0] = MatchingQueuePartition_state_unowned;
      state_MatchingQueuePartition[MatchingQueuePartition_1] = MatchingQueuePartition_state_unowned;
      state_Poller[Poller_0] = Poller_state_available;
      state_Poller[Poller_1] = Poller_state_available;
      exists_Poller += (Poller_0);
      exists_Poller += (Poller_1);
      state_WorkObligation[WorkObligation_0] = WorkObligation_state_unresolved;
      state_WorkObligation[WorkObligation_1] = WorkObligation_state_unresolved;
      CheckSafety();
      send this, eStep;
    }
    on eStep do Step;
  }

  fun Step() {
    var enabled: set[int];
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (0); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (1); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (2); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (3); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (4); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (5); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (6); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (7); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (8); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (9); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (10); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (11); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (12); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (13); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (14); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (15); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (16); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (17); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (18); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (19); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (20); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (21); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (22); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (23); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (24); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (25); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (26); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (27); }
    if (DeliveryTask_0 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (28); }
    if (DeliveryTask_1 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (29); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (30); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (31); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (32); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (33); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (34); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (35); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (36); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (37); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (38); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (39); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (40); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (41); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (42); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (43); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (44); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (45); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (46); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (47); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (48); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (49); }
    if (DeliveryTask_0 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged) { enabled += (50); }
    if (DeliveryTask_1 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged) { enabled += (51); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (52); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (53); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (54); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (55); }
    if (DeliveryTask_0 in exists_DeliveryTask && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (56); }
    if (DeliveryTask_1 in exists_DeliveryTask && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (57); }
    if (!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_0 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration)) { enabled += (58); }
    if (!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_0 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration)) { enabled += (59); }
    if (!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_1 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration)) { enabled += (60); }
    if (!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_1 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration)) { enabled += (61); }
    if (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_0 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration)) { enabled += (62); }
    if (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_0 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration)) { enabled += (63); }
    if (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_1 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration)) { enabled += (64); }
    if (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_1 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration)) { enabled += (65); }
    if (!(HistoryShard_0 in exists_HistoryShard) && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration)) { enabled += (66); }
    if (!(HistoryShard_0 in exists_HistoryShard) && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration)) { enabled += (67); }
    if (!(HistoryShard_1 in exists_HistoryShard) && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration)) { enabled += (68); }
    if (!(HistoryShard_1 in exists_HistoryShard) && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration)) { enabled += (69); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (70); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (71); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (72); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (73); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (74); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (75); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (76); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (77); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (78); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (79); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (80); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (81); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (82); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (83); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (84); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (85); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (86); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (87); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (88); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (89); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (90); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (91); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (92); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (93); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (94); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (95); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (96); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (97); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (98); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (99); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (100); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (101); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (102); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (103); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (104); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (105); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (106); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (107); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (108); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (109); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (110); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (111); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (112); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (113); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (114); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (115); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (116); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (117); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (118); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (119); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (120); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (121); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (122); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (123); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (124); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (125); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (126); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (127); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (128); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (129); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (130); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (131); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (132); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (133); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (134); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (135); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (136); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (137); }
    if (MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (138); }
    if (MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (139); }
    if (MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (140); }
    if (MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (141); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (142); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (143); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (144); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (145); }
    if (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (146); }
    if (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (147); }
    if (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (148); }
    if (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (149); }
    if (Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass) { enabled += (150); }
    if (Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass) { enabled += (151); }
    if (Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass) { enabled += (152); }
    if (Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass) { enabled += (153); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (154); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (155); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (156); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (157); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (158); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (159); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (160); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (161); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (162); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (163); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (164); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (165); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (166); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (167); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (168); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (169); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (170); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (171); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (172); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (173); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (174); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (175); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (176); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (177); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (178); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (179); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (180); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (181); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (182); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (183); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (184); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (185); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (186); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (187); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (188); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (189); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (190); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (191); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (192); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (193); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (194); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (195); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (196); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (197); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (198); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (199); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (200); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (201); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (202); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (203); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (204); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (205); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (206); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (207); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (208); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (209); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (210); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (211); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (212); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (213); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (214); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (215); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (216); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (217); }
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
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (1);
    }
    if (2 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (2);
    }
    if (3 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (3);
    }
    if (4 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (4);
    }
    if (5 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (5);
    }
    if (6 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (6);
    }
    if (7 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (7);
    }
    if (8 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (8);
    }
    if (9 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (9);
    }
    if (10 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (10);
    }
    if (11 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (11);
    }
    if (12 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (12);
    }
    if (13 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (13);
    }
    if (14 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (14);
    }
    if (15 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (15);
    }
    if (16 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (16);
    }
    if (17 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (17);
    }
    if (18 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (18);
    }
    if (19 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (19);
    }
    if (20 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (20);
    }
    if (21 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (21);
    }
    if (22 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (22);
    }
    if (23 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (23);
    }
    if (24 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (24);
    }
    if (25 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (25);
    }
    if (26 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (26);
    }
    if (27 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (27);
    }
    if (28 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      enabled -= (28);
    }
    if (29 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
      enabled -= (29);
    }
    if (30 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (30);
    }
    if (31 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (31);
    }
    if (32 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (32);
    }
    if (33 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (33);
    }
    if (34 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (34);
    }
    if (35 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (35);
    }
    if (36 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (36);
    }
    if (37 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (37);
    }
    if (38 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (38);
    }
    if (39 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (39);
    }
    if (40 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (40);
    }
    if (41 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (41);
    }
    if (42 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (42);
    }
    if (43 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (43);
    }
    if (44 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (44);
    }
    if (45 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (45);
    }
    if (46 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (46);
    }
    if (47 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (47);
    }
    if (48 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (48);
    }
    if (49 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (49);
    }
    if (50 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retire_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_retire_DeliveryTask_0(); return; }
      enabled -= (50);
    }
    if (51 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retire_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_retire_DeliveryTask_1(); return; }
      enabled -= (51);
    }
    if (52 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (52);
    }
    if (53 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (53);
    }
    if (54 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (54);
    }
    if (55 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (55);
    }
    if (56 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_spool_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_spool_DeliveryTask_0(); return; }
      enabled -= (56);
    }
    if (57 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_spool_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_spool_DeliveryTask_1(); return; }
      enabled -= (57);
    }
    if (58 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (58);
    }
    if (59 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (59);
    }
    if (60 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (60);
    }
    if (61 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (61);
    }
    if (62 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (62);
    }
    if (63 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (63);
    }
    if (64 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (64);
    }
    if (65 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (65);
    }
    if (66 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (66);
    }
    if (67 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (67);
    }
    if (68 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_history_owner_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_bootstrap_history_owner_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (68);
    }
    if (69 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_bootstrap_history_owner_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_bootstrap_history_owner_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (69);
    }
    if (70 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (70);
    }
    if (71 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (71);
    }
    if (72 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (72);
    }
    if (73 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (73);
    }
    if (74 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (74);
    }
    if (75 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (75);
    }
    if (76 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (76);
    }
    if (77 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (77);
    }
    if (78 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (78);
    }
    if (79 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (79);
    }
    if (80 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (80);
    }
    if (81 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (81);
    }
    if (82 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (82);
    }
    if (83 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (83);
    }
    if (84 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (84);
    }
    if (85 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (85);
    }
    if (86 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (86);
    }
    if (87 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (87);
    }
    if (88 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (88);
    }
    if (89 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (89);
    }
    if (90 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (90);
    }
    if (91 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (91);
    }
    if (92 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (92);
    }
    if (93 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (93);
    }
    if (94 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (94);
    }
    if (95 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (95);
    }
    if (96 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (96);
    }
    if (97 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (97);
    }
    if (98 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (98);
    }
    if (99 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (99);
    }
    if (100 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (100);
    }
    if (101 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (101);
    }
    if (102 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (102);
    }
    if (103 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (103);
    }
    if (104 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (104);
    }
    if (105 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (105);
    }
    if (106 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (106);
    }
    if (107 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (107);
    }
    if (108 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (108);
    }
    if (109 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (109);
    }
    if (110 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (110);
    }
    if (111 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (111);
    }
    if (112 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (112);
    }
    if (113 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (113);
    }
    if (114 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (114);
    }
    if (115 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (115);
    }
    if (116 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (116);
    }
    if (117 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (117);
    }
    if (118 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (118);
    }
    if (119 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (119);
    }
    if (120 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (120);
    }
    if (121 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (121);
    }
    if (122 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (122);
    }
    if (123 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (123);
    }
    if (124 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (124);
    }
    if (125 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (125);
    }
    if (126 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (126);
    }
    if (127 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (127);
    }
    if (128 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (128);
    }
    if (129 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (129);
    }
    if (130 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (130);
    }
    if (131 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (131);
    }
    if (132 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (132);
    }
    if (133 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (133);
    }
    if (134 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (134);
    }
    if (135 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (135);
    }
    if (136 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (136);
    }
    if (137 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (137);
    }
    if (138 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (138);
    }
    if (139 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (139);
    }
    if (140 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (140);
    }
    if (141 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (141);
    }
    if (142 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (142);
    }
    if (143 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (143);
    }
    if (144 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (144);
    }
    if (145 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (145);
    }
    if (146 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0(); return; }
      enabled -= (146);
    }
    if (147 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1(); return; }
      enabled -= (147);
    }
    if (148 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0(); return; }
      enabled -= (148);
    }
    if (149 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1(); return; }
      if ($) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1(); return; }
      enabled -= (149);
    }
    if (150 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_register_poller_Poller_0_DeliveryRouteClass_0(); return; }
      if ($) { Apply_routing_register_poller_Poller_0_DeliveryRouteClass_0(); return; }
      enabled -= (150);
    }
    if (151 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_register_poller_Poller_0_DeliveryRouteClass_1(); return; }
      if ($) { Apply_routing_register_poller_Poller_0_DeliveryRouteClass_1(); return; }
      enabled -= (151);
    }
    if (152 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_register_poller_Poller_1_DeliveryRouteClass_0(); return; }
      if ($) { Apply_routing_register_poller_Poller_1_DeliveryRouteClass_0(); return; }
      enabled -= (152);
    }
    if (153 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_register_poller_Poller_1_DeliveryRouteClass_1(); return; }
      if ($) { Apply_routing_register_poller_Poller_1_DeliveryRouteClass_1(); return; }
      enabled -= (153);
    }
    if (154 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (154);
    }
    if (155 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (155);
    }
    if (156 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (156);
    }
    if (157 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (157);
    }
    if (158 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (158);
    }
    if (159 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (159);
    }
    if (160 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (160);
    }
    if (161 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (161);
    }
    if (162 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (162);
    }
    if (163 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (163);
    }
    if (164 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (164);
    }
    if (165 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (165);
    }
    if (166 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (166);
    }
    if (167 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (167);
    }
    if (168 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (168);
    }
    if (169 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (169);
    }
    if (170 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (170);
    }
    if (171 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (171);
    }
    if (172 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (172);
    }
    if (173 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (173);
    }
    if (174 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (174);
    }
    if (175 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (175);
    }
    if (176 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (176);
    }
    if (177 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (177);
    }
    if (178 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (178);
    }
    if (179 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (179);
    }
    if (180 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (180);
    }
    if (181 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (181);
    }
    if (182 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (182);
    }
    if (183 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (183);
    }
    if (184 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (184);
    }
    if (185 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (185);
    }
    if (186 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (186);
    }
    if (187 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (187);
    }
    if (188 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (188);
    }
    if (189 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (189);
    }
    if (190 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (190);
    }
    if (191 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (191);
    }
    if (192 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (192);
    }
    if (193 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (193);
    }
    if (194 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (194);
    }
    if (195 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (195);
    }
    if (196 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (196);
    }
    if (197 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (197);
    }
    if (198 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (198);
    }
    if (199 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (199);
    }
    if (200 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (200);
    }
    if (201 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (201);
    }
    if (202 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (202);
    }
    if (203 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (203);
    }
    if (204 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (204);
    }
    if (205 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (205);
    }
    if (206 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (206);
    }
    if (207 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (207);
    }
    if (208 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (208);
    }
    if (209 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (209);
    }
    if (210 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (210);
    }
    if (211 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (211);
    }
    if (212 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (212);
    }
    if (213 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (213);
    }
    if (214 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
      enabled -= (214);
    }
    if (215 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
      enabled -= (215);
    }
    if (216 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
      enabled -= (216);
    }
    if (217 in enabled) {
      if (sizeof(enabled) == 1) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      if ($) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
      enabled -= (217);
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

  fun Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.acknowledge task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_acknowledged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_completed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.acknowledge task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_acknowledged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_completed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.acknowledge task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_acknowledged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_completed;
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

  fun Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.authorize.accept obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.authorize.accept obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.authorize.accept obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.authorize.accept obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.authorize.accept obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.authorize.accept obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.authorize.accept obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
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

  fun Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.authorize.reject obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.authorize.reject obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.authorize.reject obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.authorize.reject obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.authorize.reject obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.authorize.reject obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.authorize.reject obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
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

  fun Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.dispatch task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_dispatched;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_dispatched;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.dispatch task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_dispatched;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_dispatched;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.dispatch task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_dispatched;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_dispatched;
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

  fun Apply_delivery_expire_WorkObligation_0_DeliveryTask_1() {
    print "UMPIRE_ACTION delivery.expire obligation=WorkObligation#0 task=DeliveryTask#1";
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_expire_WorkObligation_1_DeliveryTask_0() {
    print "UMPIRE_ACTION delivery.expire obligation=WorkObligation#1 task=DeliveryTask#0";
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_expire_WorkObligation_1_DeliveryTask_1() {
    print "UMPIRE_ACTION delivery.expire obligation=WorkObligation#1 task=DeliveryTask#1";
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
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

  fun Apply_delivery_offer_sync_DeliveryTask_1() {
    print "UMPIRE_ACTION delivery.offer-sync task=DeliveryTask#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_sync_offered;
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

  fun Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION delivery.persist.ambiguous obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_unresolved;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION delivery.persist.ambiguous obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_unresolved;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION delivery.persist.ambiguous obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_unresolved;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION delivery.persist.ambiguous obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_unresolved;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION delivery.persist.ambiguous obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_unresolved;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION delivery.persist.ambiguous obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_unresolved;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
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

  fun Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION delivery.persist.success obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION delivery.persist.success obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION delivery.persist.success obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION delivery.persist.success obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION delivery.persist.success obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION delivery.persist.success obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
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

  fun Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1() {
    print "UMPIRE_ACTION delivery.resolve-persisted obligation=WorkObligation#0 task=DeliveryTask#1";
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0() {
    print "UMPIRE_ACTION delivery.resolve-persisted obligation=WorkObligation#1 task=DeliveryTask#0";
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1() {
    print "UMPIRE_ACTION delivery.resolve-persisted obligation=WorkObligation#1 task=DeliveryTask#1";
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
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

  fun Apply_delivery_retire_DeliveryTask_1() {
    print "UMPIRE_ACTION delivery.retire task=DeliveryTask#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
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

  fun Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.retry task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION delivery.retry task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION delivery.retry task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_failed;
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

  fun Apply_delivery_spool_DeliveryTask_1() {
    print "UMPIRE_ACTION delivery.spool task=DeliveryTask#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
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

  fun Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.bootstrap route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    exists_DeliveryRouteClass += (DeliveryRouteClass_0);
    state_DeliveryRouteClass[DeliveryRouteClass_0] = DeliveryRouteClass_state_active;
    exists_MatchingQueuePartition += (MatchingQueuePartition_1);
    state_MatchingQueuePartition[MatchingQueuePartition_1] = MatchingQueuePartition_state_owned;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_0);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_route += ((source = MatchingQueuePartition_1, target = DeliveryRouteClass_0));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.bootstrap route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    exists_DeliveryRouteClass += (DeliveryRouteClass_0);
    state_DeliveryRouteClass[DeliveryRouteClass_0] = DeliveryRouteClass_state_active;
    exists_MatchingQueuePartition += (MatchingQueuePartition_1);
    state_MatchingQueuePartition[MatchingQueuePartition_1] = MatchingQueuePartition_state_owned;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_1);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_route += ((source = MatchingQueuePartition_1, target = DeliveryRouteClass_0));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.bootstrap route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    exists_DeliveryRouteClass += (DeliveryRouteClass_1);
    state_DeliveryRouteClass[DeliveryRouteClass_1] = DeliveryRouteClass_state_active;
    exists_MatchingQueuePartition += (MatchingQueuePartition_0);
    state_MatchingQueuePartition[MatchingQueuePartition_0] = MatchingQueuePartition_state_owned;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_0);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_route += ((source = MatchingQueuePartition_0, target = DeliveryRouteClass_1));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.bootstrap route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    exists_DeliveryRouteClass += (DeliveryRouteClass_1);
    state_DeliveryRouteClass[DeliveryRouteClass_1] = DeliveryRouteClass_state_active;
    exists_MatchingQueuePartition += (MatchingQueuePartition_0);
    state_MatchingQueuePartition[MatchingQueuePartition_0] = MatchingQueuePartition_state_owned;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_1);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_route += ((source = MatchingQueuePartition_0, target = DeliveryRouteClass_1));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.bootstrap route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    exists_DeliveryRouteClass += (DeliveryRouteClass_1);
    state_DeliveryRouteClass[DeliveryRouteClass_1] = DeliveryRouteClass_state_active;
    exists_MatchingQueuePartition += (MatchingQueuePartition_1);
    state_MatchingQueuePartition[MatchingQueuePartition_1] = MatchingQueuePartition_state_owned;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_0);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_route += ((source = MatchingQueuePartition_1, target = DeliveryRouteClass_1));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.bootstrap route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    exists_DeliveryRouteClass += (DeliveryRouteClass_1);
    state_DeliveryRouteClass[DeliveryRouteClass_1] = DeliveryRouteClass_state_active;
    exists_MatchingQueuePartition += (MatchingQueuePartition_1);
    state_MatchingQueuePartition[MatchingQueuePartition_1] = MatchingQueuePartition_state_owned;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_1);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_route += ((source = MatchingQueuePartition_1, target = DeliveryRouteClass_1));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1));
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

  fun Apply_routing_bootstrap_history_owner_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.bootstrap-history-owner shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    exists_HistoryShard += (HistoryShard_1);
    state_HistoryShard[HistoryShard_1] = HistoryShard_state_owned;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_0);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner += ((source = HistoryShard_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_bootstrap_history_owner_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.bootstrap-history-owner shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    exists_HistoryShard += (HistoryShard_1);
    state_HistoryShard[HistoryShard_1] = HistoryShard_state_owned;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_1);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner += ((source = HistoryShard_1, target = HistoryOwnerGeneration_1));
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

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
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

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_0, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_0, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_0, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_0, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_0));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_0));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#0 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_0));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#0";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.forward-to-matching task=DeliveryTask#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1 shard=HistoryShard#1 historyGeneration=HistoryOwnerGeneration#1";
    relation_delivery_task_route += ((source = DeliveryTask_1, target = DeliveryRouteClass_1));
    relation_delivery_task_owner_generation += ((source = DeliveryTask_1, target = MatchingOwnerGeneration_1));
    relation_delivery_task_history_shard += ((source = DeliveryTask_1, target = HistoryShard_1));
    relation_delivery_task_history_owner_generation += ((source = DeliveryTask_1, target = HistoryOwnerGeneration_1));
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

  fun Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.handoff partition=MatchingQueuePartition#1 oldGeneration=MatchingOwnerGeneration#0 newGeneration=MatchingOwnerGeneration#0";
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_stale;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_0);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_owner -= ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.handoff partition=MatchingQueuePartition#1 oldGeneration=MatchingOwnerGeneration#0 newGeneration=MatchingOwnerGeneration#1";
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_stale;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_1);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_owner -= ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.handoff partition=MatchingQueuePartition#1 oldGeneration=MatchingOwnerGeneration#1 newGeneration=MatchingOwnerGeneration#0";
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_stale;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_0);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_owner -= ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.handoff partition=MatchingQueuePartition#1 oldGeneration=MatchingOwnerGeneration#1 newGeneration=MatchingOwnerGeneration#1";
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_stale;
    exists_MatchingOwnerGeneration += (MatchingOwnerGeneration_1);
    state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] = MatchingOwnerGeneration_state_current;
    relation_delivery_partition_owner -= ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1));
    relation_delivery_partition_owner += ((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1));
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

  fun Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.handoff-history-owner shard=HistoryShard#1 oldHistoryGeneration=HistoryOwnerGeneration#0 newHistoryGeneration=HistoryOwnerGeneration#0";
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_stale;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_0);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner -= ((source = HistoryShard_1, target = HistoryOwnerGeneration_0));
    relation_history_shard_owner += ((source = HistoryShard_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.handoff-history-owner shard=HistoryShard#1 oldHistoryGeneration=HistoryOwnerGeneration#0 newHistoryGeneration=HistoryOwnerGeneration#1";
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_stale;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_1);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner -= ((source = HistoryShard_1, target = HistoryOwnerGeneration_0));
    relation_history_shard_owner += ((source = HistoryShard_1, target = HistoryOwnerGeneration_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.handoff-history-owner shard=HistoryShard#1 oldHistoryGeneration=HistoryOwnerGeneration#1 newHistoryGeneration=HistoryOwnerGeneration#0";
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_stale;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_0);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner -= ((source = HistoryShard_1, target = HistoryOwnerGeneration_1));
    relation_history_shard_owner += ((source = HistoryShard_1, target = HistoryOwnerGeneration_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.handoff-history-owner shard=HistoryShard#1 oldHistoryGeneration=HistoryOwnerGeneration#1 newHistoryGeneration=HistoryOwnerGeneration#1";
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_stale;
    exists_HistoryOwnerGeneration += (HistoryOwnerGeneration_1);
    state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] = HistoryOwnerGeneration_state_current;
    relation_history_shard_owner -= ((source = HistoryShard_1, target = HistoryOwnerGeneration_1));
    relation_history_shard_owner += ((source = HistoryShard_1, target = HistoryOwnerGeneration_1));
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

  fun Apply_routing_register_poller_Poller_0_DeliveryRouteClass_1() {
    print "UMPIRE_ACTION routing.register-poller poller=Poller#0 route=DeliveryRouteClass#1";
    relation_delivery_poller_route += ((source = Poller_0, target = DeliveryRouteClass_1));
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

  fun Apply_routing_register_poller_Poller_1_DeliveryRouteClass_1() {
    print "UMPIRE_ACTION routing.register-poller poller=Poller#1 route=DeliveryRouteClass#1";
    relation_delivery_poller_route += ((source = Poller_1, target = DeliveryRouteClass_1));
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

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
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

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#0 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#0 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#0 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1() {
    print "UMPIRE_ACTION routing.reserve-compatible task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#1 route=DeliveryRouteClass#1 partition=MatchingQueuePartition#1 generation=MatchingOwnerGeneration#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun CheckSafety() {
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start) || (WorkObligation_0 in exists_WorkObligation && DeliveryAttempt_0 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start) || (WorkObligation_0 in exists_WorkObligation && DeliveryAttempt_1 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start) || (WorkObligation_1 in exists_WorkObligation && DeliveryAttempt_0 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start) || (WorkObligation_1 in exists_WorkObligation && DeliveryAttempt_1 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start && (source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds source cardinality";
    assert !((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start && (source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds source cardinality";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start && (source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds target cardinality";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start && (source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds target cardinality";
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_0 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_1 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller) || (DeliveryAttempt_1 in exists_DeliveryAttempt && Poller_0 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller) || (DeliveryAttempt_1 in exists_DeliveryAttempt && Poller_1 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller && (source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller), "relation delivery-attempt-poller exceeds source cardinality";
    assert !((source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller && (source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller), "relation delivery-attempt-poller exceeds source cardinality";
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task) || (DeliveryAttempt_0 in exists_DeliveryAttempt && DeliveryTask_0 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task) || (DeliveryAttempt_0 in exists_DeliveryAttempt && DeliveryTask_1 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task) || (DeliveryAttempt_1 in exists_DeliveryAttempt && DeliveryTask_0 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task) || (DeliveryAttempt_1 in exists_DeliveryAttempt && DeliveryTask_1 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task), "relation delivery-attempt-task exceeds source cardinality";
    assert !((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task), "relation delivery-attempt-task exceeds source cardinality";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner) || (MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner) || (MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner), "relation delivery-partition-owner exceeds source cardinality";
    assert !((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner), "relation delivery-partition-owner exceeds source cardinality";
    assert !((source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
    assert !((source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route) || (MatchingQueuePartition_1 in exists_MatchingQueuePartition && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
    assert !((source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route) || (MatchingQueuePartition_1 in exists_MatchingQueuePartition && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route), "relation delivery-partition-route exceeds source cardinality";
    assert !((source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route), "relation delivery-partition-route exceeds source cardinality";
    assert !((source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route) || (Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route) || (Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route) || (Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route) || (Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route), "relation delivery-poller-route exceeds source cardinality";
    assert !((source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route), "relation delivery-poller-route exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || (DeliveryTask_1 in exists_DeliveryTask && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || (DeliveryTask_1 in exists_DeliveryTask && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && (source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation), "relation delivery-task-history-owner-generation exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && (source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation), "relation delivery-task-history-owner-generation exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = HistoryShard_0) in relation_delivery_task_history_shard) || (DeliveryTask_0 in exists_DeliveryTask && HistoryShard_0 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryShard_1) in relation_delivery_task_history_shard) || (DeliveryTask_0 in exists_DeliveryTask && HistoryShard_1 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
    assert !((source = DeliveryTask_1, target = HistoryShard_0) in relation_delivery_task_history_shard) || (DeliveryTask_1 in exists_DeliveryTask && HistoryShard_0 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
    assert !((source = DeliveryTask_1, target = HistoryShard_1) in relation_delivery_task_history_shard) || (DeliveryTask_1 in exists_DeliveryTask && HistoryShard_1 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryShard_0) in relation_delivery_task_history_shard && (source = DeliveryTask_0, target = HistoryShard_1) in relation_delivery_task_history_shard), "relation delivery-task-history-shard exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = HistoryShard_0) in relation_delivery_task_history_shard && (source = DeliveryTask_1, target = HistoryShard_1) in relation_delivery_task_history_shard), "relation delivery-task-history-shard exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || (DeliveryTask_1 in exists_DeliveryTask && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || (DeliveryTask_1 in exists_DeliveryTask && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation), "relation delivery-task-owner-generation exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation), "relation delivery-task-owner-generation exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route), "relation delivery-task-route exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route), "relation delivery-task-route exceeds source cardinality";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner) || (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner) || (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner) || (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner) || (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner), "relation history-shard-owner exceeds source cardinality";
    assert !((source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner), "relation history-shard-owner exceeds source cardinality";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal)))))))))), "property delivery.coarse-retirement-safety failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue)))))), "property delivery.destination-isolation failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))))), "property delivery.failed-start-is-not-accepted failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))))))))), "property delivery.no-phantom-dispatch failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired))))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))))), "property delivery.no-resurrection failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))), "property delivery.no-split-commit failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current)))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current))))))))))))), "property delivery.owner-generation-fencing failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue)))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue))))))), "property delivery.path-equivalence failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))))), "property delivery.retry-preserves-obligation failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((!(DeliveryTask_0 in exists_DeliveryTask) || (((!(Poller_0 in exists_Poller) || ((!(((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))) && (!(Poller_1 in exists_Poller) || ((!(((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((!(Poller_0 in exists_Poller) || ((!(((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))) && (!(Poller_1 in exists_Poller) || ((!(((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || (((!(DeliveryTask_0 in exists_DeliveryTask) || (((!(Poller_0 in exists_Poller) || ((!(((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))) && (!(Poller_1 in exists_Poller) || ((!(((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((!(Poller_0 in exists_Poller) || ((!(((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))) && (!(Poller_1 in exists_Poller) || ((!(((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))))))))))))), "property delivery.routing-isolation failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))))), "property delivery.single-accepted-start failed";
  }

  fun CheckQuiescent() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved))) && (!(WorkObligation_1 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved)))), "quiescent property delivery.ambiguous-commit-resolved failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
