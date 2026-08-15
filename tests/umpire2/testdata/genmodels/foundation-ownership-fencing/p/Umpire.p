// Generated from the Umpire verification snapshot. Do not edit.

event eStep;
type tSelection = (chosen: int, remaining: set[int]);

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
    var selection: tSelection;
    enabled = EnabledChunk_0(enabled);
    enabled = EnabledChunk_1(enabled);
    enabled = EnabledChunk_2(enabled);
    if (sizeof(enabled) == 0) {
      CheckQuiescent();
      raise halt;
    }
    selection = SelectChunk_0(enabled);
    if (selection.chosen >= 0) { ApplyChunk_0(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_1(enabled);
    if (selection.chosen >= 0) { ApplyChunk_1(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_2(enabled);
    if (selection.chosen >= 0) { ApplyChunk_2(selection.chosen); return; }
    enabled = selection.remaining;
  }

  fun EnabledChunk_0(enabled: set[int]): set[int] {
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
    return enabled;
  }

  fun SelectChunk_0(enabled: set[int]): tSelection {
    if (0 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 0, remaining = enabled); }
      if ($) { return (chosen = 0, remaining = enabled); }
      enabled -= (0);
    }
    if (1 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 1, remaining = enabled); }
      if ($) { return (chosen = 1, remaining = enabled); }
      enabled -= (1);
    }
    if (2 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 2, remaining = enabled); }
      if ($) { return (chosen = 2, remaining = enabled); }
      enabled -= (2);
    }
    if (3 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 3, remaining = enabled); }
      if ($) { return (chosen = 3, remaining = enabled); }
      enabled -= (3);
    }
    if (4 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 4, remaining = enabled); }
      if ($) { return (chosen = 4, remaining = enabled); }
      enabled -= (4);
    }
    if (5 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 5, remaining = enabled); }
      if ($) { return (chosen = 5, remaining = enabled); }
      enabled -= (5);
    }
    if (6 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 6, remaining = enabled); }
      if ($) { return (chosen = 6, remaining = enabled); }
      enabled -= (6);
    }
    if (7 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 7, remaining = enabled); }
      if ($) { return (chosen = 7, remaining = enabled); }
      enabled -= (7);
    }
    if (8 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 8, remaining = enabled); }
      if ($) { return (chosen = 8, remaining = enabled); }
      enabled -= (8);
    }
    if (9 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 9, remaining = enabled); }
      if ($) { return (chosen = 9, remaining = enabled); }
      enabled -= (9);
    }
    if (10 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 10, remaining = enabled); }
      if ($) { return (chosen = 10, remaining = enabled); }
      enabled -= (10);
    }
    if (11 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 11, remaining = enabled); }
      if ($) { return (chosen = 11, remaining = enabled); }
      enabled -= (11);
    }
    if (12 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 12, remaining = enabled); }
      if ($) { return (chosen = 12, remaining = enabled); }
      enabled -= (12);
    }
    if (13 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 13, remaining = enabled); }
      if ($) { return (chosen = 13, remaining = enabled); }
      enabled -= (13);
    }
    if (14 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 14, remaining = enabled); }
      if ($) { return (chosen = 14, remaining = enabled); }
      enabled -= (14);
    }
    if (15 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 15, remaining = enabled); }
      if ($) { return (chosen = 15, remaining = enabled); }
      enabled -= (15);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_0(selected: int) {
    if (selected == 0) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 2) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 3) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 4) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 5) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
    if (selected == 6) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 7) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 8) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 9) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 10) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 11) { Apply_delivery_retire_DeliveryTask_0(); return; }
    if (selected == 12) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 13) { Apply_delivery_spool_DeliveryTask_0(); return; }
    if (selected == 14) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 15) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_1(enabled: set[int]): set[int] {
    if (!(HistoryShard_0 in exists_HistoryShard) && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration)) { enabled += (16); }
    if (!(HistoryShard_0 in exists_HistoryShard) && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration)) { enabled += (17); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (18); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (19); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (20); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (21); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (22); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (23); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (24); }
    if (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration) && (state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (25); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (26); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (27); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (28); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))) { enabled += (29); }
    if (Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))))) { enabled += (30); }
    if (Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))))) { enabled += (31); }
    return enabled;
  }

  fun SelectChunk_1(enabled: set[int]): tSelection {
    if (16 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 16, remaining = enabled); }
      if ($) { return (chosen = 16, remaining = enabled); }
      enabled -= (16);
    }
    if (17 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 17, remaining = enabled); }
      if ($) { return (chosen = 17, remaining = enabled); }
      enabled -= (17);
    }
    if (18 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 18, remaining = enabled); }
      if ($) { return (chosen = 18, remaining = enabled); }
      enabled -= (18);
    }
    if (19 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 19, remaining = enabled); }
      if ($) { return (chosen = 19, remaining = enabled); }
      enabled -= (19);
    }
    if (20 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 20, remaining = enabled); }
      if ($) { return (chosen = 20, remaining = enabled); }
      enabled -= (20);
    }
    if (21 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 21, remaining = enabled); }
      if ($) { return (chosen = 21, remaining = enabled); }
      enabled -= (21);
    }
    if (22 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 22, remaining = enabled); }
      if ($) { return (chosen = 22, remaining = enabled); }
      enabled -= (22);
    }
    if (23 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 23, remaining = enabled); }
      if ($) { return (chosen = 23, remaining = enabled); }
      enabled -= (23);
    }
    if (24 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 24, remaining = enabled); }
      if ($) { return (chosen = 24, remaining = enabled); }
      enabled -= (24);
    }
    if (25 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 25, remaining = enabled); }
      if ($) { return (chosen = 25, remaining = enabled); }
      enabled -= (25);
    }
    if (26 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 26, remaining = enabled); }
      if ($) { return (chosen = 26, remaining = enabled); }
      enabled -= (26);
    }
    if (27 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 27, remaining = enabled); }
      if ($) { return (chosen = 27, remaining = enabled); }
      enabled -= (27);
    }
    if (28 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 28, remaining = enabled); }
      if ($) { return (chosen = 28, remaining = enabled); }
      enabled -= (28);
    }
    if (29 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 29, remaining = enabled); }
      if ($) { return (chosen = 29, remaining = enabled); }
      enabled -= (29);
    }
    if (30 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 30, remaining = enabled); }
      if ($) { return (chosen = 30, remaining = enabled); }
      enabled -= (30);
    }
    if (31 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 31, remaining = enabled); }
      if ($) { return (chosen = 31, remaining = enabled); }
      enabled -= (31);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_1(selected: int) {
    if (selected == 16) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 17) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 18) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 19) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 20) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 21) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 22) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 23) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 24) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 25) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 26) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 27) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 28) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 29) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 30) { Apply_routing_register_poller_Poller_0_DeliveryRouteClass_0(); return; }
    if (selected == 31) { Apply_routing_register_poller_Poller_1_DeliveryRouteClass_0(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_2(enabled: set[int]): set[int] {
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (32); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (33); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (34); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (35); }
    return enabled;
  }

  fun SelectChunk_2(enabled: set[int]): tSelection {
    if (32 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 32, remaining = enabled); }
      if ($) { return (chosen = 32, remaining = enabled); }
      enabled -= (32);
    }
    if (33 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 33, remaining = enabled); }
      if ($) { return (chosen = 33, remaining = enabled); }
      enabled -= (33);
    }
    if (34 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 34, remaining = enabled); }
      if ($) { return (chosen = 34, remaining = enabled); }
      enabled -= (34);
    }
    if (35 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 35, remaining = enabled); }
      if ($) { return (chosen = 35, remaining = enabled); }
      enabled -= (35);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_2(selected: int) {
    if (selected == 32) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 33) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 34) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 35) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
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
    CheckRelation_0();
    CheckRelation_1();
    CheckRelation_2();
    CheckRelation_3();
    CheckRelation_4();
    CheckRelation_5();
    CheckRelation_6();
    CheckRelation_7();
    CheckRelation_8();
    CheckRelation_9();
    CheckRelation_10();
    CheckRelation_11();
    CheckRelation_12();
    CheckProperty_0();
    CheckProperty_1();
    CheckProperty_2();
    CheckProperty_3();
    CheckProperty_4();
    CheckProperty_5();
    CheckProperty_6();
    CheckProperty_7();
    CheckProperty_8();
    CheckProperty_9();
  }

  fun CheckRelation_0() {
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start) || (WorkObligation_0 in exists_WorkObligation && DeliveryAttempt_0 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
  }

  fun CheckRelation_1() {
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_0 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_1 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller && (source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller), "relation delivery-attempt-poller exceeds source cardinality";
  }

  fun CheckRelation_2() {
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task) || (DeliveryAttempt_0 in exists_DeliveryAttempt && DeliveryTask_0 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
  }

  fun CheckRelation_3() {
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner), "relation delivery-partition-owner exceeds source cardinality";
  }

  fun CheckRelation_4() {
    assert !((source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
  }

  fun CheckRelation_5() {
    assert !((source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route) || (Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route) || (Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
  }

  fun CheckRelation_6() {
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && (source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation), "relation delivery-task-history-owner-generation exceeds source cardinality";
  }

  fun CheckRelation_7() {
    assert !((source = DeliveryTask_0, target = HistoryShard_0) in relation_delivery_task_history_shard) || (DeliveryTask_0 in exists_DeliveryTask && HistoryShard_0 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
  }

  fun CheckRelation_8() {
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
  }

  fun CheckRelation_9() {
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation), "relation delivery-task-owner-generation exceeds source cardinality";
  }

  fun CheckRelation_10() {
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
  }

  fun CheckRelation_11() {
    assert !((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
  }

  fun CheckRelation_12() {
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner) || (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner) || (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner), "relation history-shard-owner exceeds source cardinality";
  }

  fun CheckProperty_0() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))))))))), "property delivery.coarse-retirement-safety failed";
  }

  fun CheckProperty_1() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue)))))), "property delivery.destination-isolation failed";
  }

  fun CheckProperty_2() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start))))))))), "property delivery.failed-start-is-not-accepted failed";
  }

  fun CheckProperty_3() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))))))))))))), "property delivery.no-phantom-dispatch failed";
  }

  fun CheckProperty_4() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))))))))), "property delivery.no-resurrection failed";
  }

  fun CheckProperty_5() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)))))))), "property delivery.no-split-commit failed";
  }

  fun CheckProperty_6() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current))))))))))))), "property delivery.owner-generation-fencing failed";
  }

  fun CheckProperty_7() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue))))))), "property delivery.path-equivalence failed";
  }

  fun CheckProperty_8() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)))))))))), "property delivery.retry-preserves-obligation failed";
  }

  fun CheckProperty_9() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)))))))), "property delivery.single-accepted-start failed";
  }

  fun CheckQuiescent() {
    CheckQuiescentProperty_0();
  }

  fun CheckQuiescentProperty_0() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved)))), "quiescent property delivery.ambiguous-commit-resolved failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
