// Generated from the Umpire verification snapshot. Do not edit.

event eStep;
type tSelection = (chosen: int, remaining: set[int]);

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
    var selection: tSelection;
    enabled = EnabledChunk_0(enabled);
    enabled = EnabledChunk_1(enabled);
    enabled = EnabledChunk_2(enabled);
    enabled = EnabledChunk_3(enabled);
    enabled = EnabledChunk_4(enabled);
    enabled = EnabledChunk_5(enabled);
    enabled = EnabledChunk_6(enabled);
    enabled = EnabledChunk_7(enabled);
    enabled = EnabledChunk_8(enabled);
    enabled = EnabledChunk_9(enabled);
    enabled = EnabledChunk_10(enabled);
    enabled = EnabledChunk_11(enabled);
    enabled = EnabledChunk_12(enabled);
    enabled = EnabledChunk_13(enabled);
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
    selection = SelectChunk_3(enabled);
    if (selection.chosen >= 0) { ApplyChunk_3(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_4(enabled);
    if (selection.chosen >= 0) { ApplyChunk_4(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_5(enabled);
    if (selection.chosen >= 0) { ApplyChunk_5(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_6(enabled);
    if (selection.chosen >= 0) { ApplyChunk_6(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_7(enabled);
    if (selection.chosen >= 0) { ApplyChunk_7(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_8(enabled);
    if (selection.chosen >= 0) { ApplyChunk_8(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_9(enabled);
    if (selection.chosen >= 0) { ApplyChunk_9(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_10(enabled);
    if (selection.chosen >= 0) { ApplyChunk_10(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_11(enabled);
    if (selection.chosen >= 0) { ApplyChunk_11(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_12(enabled);
    if (selection.chosen >= 0) { ApplyChunk_12(selection.chosen); return; }
    enabled = selection.remaining;
    selection = SelectChunk_13(enabled);
    if (selection.chosen >= 0) { ApplyChunk_13(selection.chosen); return; }
    enabled = selection.remaining;
  }

  fun EnabledChunk_0(enabled: set[int]): set[int] {
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
    if (selected == 1) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 2) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 3) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 4) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 5) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 6) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 7) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 8) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 9) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 10) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 11) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 12) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 13) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 14) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 15) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_1(enabled: set[int]): set[int] {
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
    if (selected == 16) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 17) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 18) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 19) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 20) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 21) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 22) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 23) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 24) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 25) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_1(); return; }
    if (selected == 26) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_0(); return; }
    if (selected == 27) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_1(); return; }
    if (selected == 28) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
    if (selected == 29) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
    if (selected == 30) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 31) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_2(enabled: set[int]): set[int] {
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
    if (36 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 36, remaining = enabled); }
      if ($) { return (chosen = 36, remaining = enabled); }
      enabled -= (36);
    }
    if (37 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 37, remaining = enabled); }
      if ($) { return (chosen = 37, remaining = enabled); }
      enabled -= (37);
    }
    if (38 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 38, remaining = enabled); }
      if ($) { return (chosen = 38, remaining = enabled); }
      enabled -= (38);
    }
    if (39 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 39, remaining = enabled); }
      if ($) { return (chosen = 39, remaining = enabled); }
      enabled -= (39);
    }
    if (40 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 40, remaining = enabled); }
      if ($) { return (chosen = 40, remaining = enabled); }
      enabled -= (40);
    }
    if (41 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 41, remaining = enabled); }
      if ($) { return (chosen = 41, remaining = enabled); }
      enabled -= (41);
    }
    if (42 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 42, remaining = enabled); }
      if ($) { return (chosen = 42, remaining = enabled); }
      enabled -= (42);
    }
    if (43 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 43, remaining = enabled); }
      if ($) { return (chosen = 43, remaining = enabled); }
      enabled -= (43);
    }
    if (44 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 44, remaining = enabled); }
      if ($) { return (chosen = 44, remaining = enabled); }
      enabled -= (44);
    }
    if (45 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 45, remaining = enabled); }
      if ($) { return (chosen = 45, remaining = enabled); }
      enabled -= (45);
    }
    if (46 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 46, remaining = enabled); }
      if ($) { return (chosen = 46, remaining = enabled); }
      enabled -= (46);
    }
    if (47 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 47, remaining = enabled); }
      if ($) { return (chosen = 47, remaining = enabled); }
      enabled -= (47);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_2(selected: int) {
    if (selected == 32) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 33) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 34) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 35) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 36) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 37) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 38) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 39) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 40) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 41) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 42) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 43) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 44) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 45) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 46) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 47) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_3(enabled: set[int]): set[int] {
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
    return enabled;
  }

  fun SelectChunk_3(enabled: set[int]): tSelection {
    if (48 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 48, remaining = enabled); }
      if ($) { return (chosen = 48, remaining = enabled); }
      enabled -= (48);
    }
    if (49 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 49, remaining = enabled); }
      if ($) { return (chosen = 49, remaining = enabled); }
      enabled -= (49);
    }
    if (50 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 50, remaining = enabled); }
      if ($) { return (chosen = 50, remaining = enabled); }
      enabled -= (50);
    }
    if (51 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 51, remaining = enabled); }
      if ($) { return (chosen = 51, remaining = enabled); }
      enabled -= (51);
    }
    if (52 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 52, remaining = enabled); }
      if ($) { return (chosen = 52, remaining = enabled); }
      enabled -= (52);
    }
    if (53 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 53, remaining = enabled); }
      if ($) { return (chosen = 53, remaining = enabled); }
      enabled -= (53);
    }
    if (54 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 54, remaining = enabled); }
      if ($) { return (chosen = 54, remaining = enabled); }
      enabled -= (54);
    }
    if (55 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 55, remaining = enabled); }
      if ($) { return (chosen = 55, remaining = enabled); }
      enabled -= (55);
    }
    if (56 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 56, remaining = enabled); }
      if ($) { return (chosen = 56, remaining = enabled); }
      enabled -= (56);
    }
    if (57 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 57, remaining = enabled); }
      if ($) { return (chosen = 57, remaining = enabled); }
      enabled -= (57);
    }
    if (58 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 58, remaining = enabled); }
      if ($) { return (chosen = 58, remaining = enabled); }
      enabled -= (58);
    }
    if (59 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 59, remaining = enabled); }
      if ($) { return (chosen = 59, remaining = enabled); }
      enabled -= (59);
    }
    if (60 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 60, remaining = enabled); }
      if ($) { return (chosen = 60, remaining = enabled); }
      enabled -= (60);
    }
    if (61 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 61, remaining = enabled); }
      if ($) { return (chosen = 61, remaining = enabled); }
      enabled -= (61);
    }
    if (62 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 62, remaining = enabled); }
      if ($) { return (chosen = 62, remaining = enabled); }
      enabled -= (62);
    }
    if (63 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 63, remaining = enabled); }
      if ($) { return (chosen = 63, remaining = enabled); }
      enabled -= (63);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_3(selected: int) {
    if (selected == 48) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0(); return; }
    if (selected == 49) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1(); return; }
    if (selected == 50) { Apply_delivery_retire_DeliveryTask_0(); return; }
    if (selected == 51) { Apply_delivery_retire_DeliveryTask_1(); return; }
    if (selected == 52) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 53) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 54) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 55) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 56) { Apply_delivery_spool_DeliveryTask_0(); return; }
    if (selected == 57) { Apply_delivery_spool_DeliveryTask_1(); return; }
    if (selected == 58) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 59) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 60) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 61) { Apply_routing_bootstrap_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 62) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 63) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_4(enabled: set[int]): set[int] {
    if (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_1 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration)) { enabled += (64); }
    if (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) && !(MatchingQueuePartition_1 in exists_MatchingQueuePartition) && !(MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration)) { enabled += (65); }
    if (!(HistoryShard_0 in exists_HistoryShard) && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration)) { enabled += (66); }
    if (!(HistoryShard_0 in exists_HistoryShard) && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration)) { enabled += (67); }
    if (!(HistoryShard_1 in exists_HistoryShard) && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration)) { enabled += (68); }
    if (!(HistoryShard_1 in exists_HistoryShard) && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration)) { enabled += (69); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (70); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (71); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (72); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (73); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (74); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (75); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (76); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (77); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (78); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (79); }
    return enabled;
  }

  fun SelectChunk_4(enabled: set[int]): tSelection {
    if (64 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 64, remaining = enabled); }
      if ($) { return (chosen = 64, remaining = enabled); }
      enabled -= (64);
    }
    if (65 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 65, remaining = enabled); }
      if ($) { return (chosen = 65, remaining = enabled); }
      enabled -= (65);
    }
    if (66 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 66, remaining = enabled); }
      if ($) { return (chosen = 66, remaining = enabled); }
      enabled -= (66);
    }
    if (67 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 67, remaining = enabled); }
      if ($) { return (chosen = 67, remaining = enabled); }
      enabled -= (67);
    }
    if (68 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 68, remaining = enabled); }
      if ($) { return (chosen = 68, remaining = enabled); }
      enabled -= (68);
    }
    if (69 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 69, remaining = enabled); }
      if ($) { return (chosen = 69, remaining = enabled); }
      enabled -= (69);
    }
    if (70 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 70, remaining = enabled); }
      if ($) { return (chosen = 70, remaining = enabled); }
      enabled -= (70);
    }
    if (71 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 71, remaining = enabled); }
      if ($) { return (chosen = 71, remaining = enabled); }
      enabled -= (71);
    }
    if (72 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 72, remaining = enabled); }
      if ($) { return (chosen = 72, remaining = enabled); }
      enabled -= (72);
    }
    if (73 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 73, remaining = enabled); }
      if ($) { return (chosen = 73, remaining = enabled); }
      enabled -= (73);
    }
    if (74 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 74, remaining = enabled); }
      if ($) { return (chosen = 74, remaining = enabled); }
      enabled -= (74);
    }
    if (75 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 75, remaining = enabled); }
      if ($) { return (chosen = 75, remaining = enabled); }
      enabled -= (75);
    }
    if (76 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 76, remaining = enabled); }
      if ($) { return (chosen = 76, remaining = enabled); }
      enabled -= (76);
    }
    if (77 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 77, remaining = enabled); }
      if ($) { return (chosen = 77, remaining = enabled); }
      enabled -= (77);
    }
    if (78 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 78, remaining = enabled); }
      if ($) { return (chosen = 78, remaining = enabled); }
      enabled -= (78);
    }
    if (79 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 79, remaining = enabled); }
      if ($) { return (chosen = 79, remaining = enabled); }
      enabled -= (79);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_4(selected: int) {
    if (selected == 64) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 65) { Apply_routing_bootstrap_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 66) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 67) { Apply_routing_bootstrap_history_owner_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 68) { Apply_routing_bootstrap_history_owner_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 69) { Apply_routing_bootstrap_history_owner_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 70) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 71) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 72) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 73) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 74) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 75) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 76) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 77) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 78) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 79) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_5(enabled: set[int]): set[int] {
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (80); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (81); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (82); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (83); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (84); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (85); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (86); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (87); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (88); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (89); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (90); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (91); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (92); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (93); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (94); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (95); }
    return enabled;
  }

  fun SelectChunk_5(enabled: set[int]): tSelection {
    if (80 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 80, remaining = enabled); }
      if ($) { return (chosen = 80, remaining = enabled); }
      enabled -= (80);
    }
    if (81 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 81, remaining = enabled); }
      if ($) { return (chosen = 81, remaining = enabled); }
      enabled -= (81);
    }
    if (82 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 82, remaining = enabled); }
      if ($) { return (chosen = 82, remaining = enabled); }
      enabled -= (82);
    }
    if (83 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 83, remaining = enabled); }
      if ($) { return (chosen = 83, remaining = enabled); }
      enabled -= (83);
    }
    if (84 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 84, remaining = enabled); }
      if ($) { return (chosen = 84, remaining = enabled); }
      enabled -= (84);
    }
    if (85 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 85, remaining = enabled); }
      if ($) { return (chosen = 85, remaining = enabled); }
      enabled -= (85);
    }
    if (86 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 86, remaining = enabled); }
      if ($) { return (chosen = 86, remaining = enabled); }
      enabled -= (86);
    }
    if (87 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 87, remaining = enabled); }
      if ($) { return (chosen = 87, remaining = enabled); }
      enabled -= (87);
    }
    if (88 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 88, remaining = enabled); }
      if ($) { return (chosen = 88, remaining = enabled); }
      enabled -= (88);
    }
    if (89 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 89, remaining = enabled); }
      if ($) { return (chosen = 89, remaining = enabled); }
      enabled -= (89);
    }
    if (90 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 90, remaining = enabled); }
      if ($) { return (chosen = 90, remaining = enabled); }
      enabled -= (90);
    }
    if (91 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 91, remaining = enabled); }
      if ($) { return (chosen = 91, remaining = enabled); }
      enabled -= (91);
    }
    if (92 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 92, remaining = enabled); }
      if ($) { return (chosen = 92, remaining = enabled); }
      enabled -= (92);
    }
    if (93 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 93, remaining = enabled); }
      if ($) { return (chosen = 93, remaining = enabled); }
      enabled -= (93);
    }
    if (94 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 94, remaining = enabled); }
      if ($) { return (chosen = 94, remaining = enabled); }
      enabled -= (94);
    }
    if (95 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 95, remaining = enabled); }
      if ($) { return (chosen = 95, remaining = enabled); }
      enabled -= (95);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_5(selected: int) {
    if (selected == 80) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 81) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 82) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 83) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 84) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 85) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 86) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 87) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 88) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 89) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 90) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 91) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 92) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 93) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 94) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 95) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_6(enabled: set[int]): set[int] {
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (96); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (97); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (98); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (99); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (100); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (101); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (102); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (103); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (104); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (105); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (106); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (107); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (108); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (109); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (110); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (111); }
    return enabled;
  }

  fun SelectChunk_6(enabled: set[int]): tSelection {
    if (96 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 96, remaining = enabled); }
      if ($) { return (chosen = 96, remaining = enabled); }
      enabled -= (96);
    }
    if (97 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 97, remaining = enabled); }
      if ($) { return (chosen = 97, remaining = enabled); }
      enabled -= (97);
    }
    if (98 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 98, remaining = enabled); }
      if ($) { return (chosen = 98, remaining = enabled); }
      enabled -= (98);
    }
    if (99 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 99, remaining = enabled); }
      if ($) { return (chosen = 99, remaining = enabled); }
      enabled -= (99);
    }
    if (100 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 100, remaining = enabled); }
      if ($) { return (chosen = 100, remaining = enabled); }
      enabled -= (100);
    }
    if (101 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 101, remaining = enabled); }
      if ($) { return (chosen = 101, remaining = enabled); }
      enabled -= (101);
    }
    if (102 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 102, remaining = enabled); }
      if ($) { return (chosen = 102, remaining = enabled); }
      enabled -= (102);
    }
    if (103 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 103, remaining = enabled); }
      if ($) { return (chosen = 103, remaining = enabled); }
      enabled -= (103);
    }
    if (104 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 104, remaining = enabled); }
      if ($) { return (chosen = 104, remaining = enabled); }
      enabled -= (104);
    }
    if (105 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 105, remaining = enabled); }
      if ($) { return (chosen = 105, remaining = enabled); }
      enabled -= (105);
    }
    if (106 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 106, remaining = enabled); }
      if ($) { return (chosen = 106, remaining = enabled); }
      enabled -= (106);
    }
    if (107 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 107, remaining = enabled); }
      if ($) { return (chosen = 107, remaining = enabled); }
      enabled -= (107);
    }
    if (108 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 108, remaining = enabled); }
      if ($) { return (chosen = 108, remaining = enabled); }
      enabled -= (108);
    }
    if (109 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 109, remaining = enabled); }
      if ($) { return (chosen = 109, remaining = enabled); }
      enabled -= (109);
    }
    if (110 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 110, remaining = enabled); }
      if ($) { return (chosen = 110, remaining = enabled); }
      enabled -= (110);
    }
    if (111 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 111, remaining = enabled); }
      if ($) { return (chosen = 111, remaining = enabled); }
      enabled -= (111);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_6(selected: int) {
    if (selected == 96) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 97) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 98) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 99) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 100) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 101) { Apply_routing_forward_to_matching_DeliveryTask_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 102) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 103) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 104) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 105) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 106) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 107) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 108) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 109) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 110) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 111) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_7(enabled: set[int]): set[int] {
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (112); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (113); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (114); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (115); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (116); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (117); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (118); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (119); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (120); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (121); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (122); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (123); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (124); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (125); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (126); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (127); }
    return enabled;
  }

  fun SelectChunk_7(enabled: set[int]): tSelection {
    if (112 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 112, remaining = enabled); }
      if ($) { return (chosen = 112, remaining = enabled); }
      enabled -= (112);
    }
    if (113 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 113, remaining = enabled); }
      if ($) { return (chosen = 113, remaining = enabled); }
      enabled -= (113);
    }
    if (114 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 114, remaining = enabled); }
      if ($) { return (chosen = 114, remaining = enabled); }
      enabled -= (114);
    }
    if (115 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 115, remaining = enabled); }
      if ($) { return (chosen = 115, remaining = enabled); }
      enabled -= (115);
    }
    if (116 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 116, remaining = enabled); }
      if ($) { return (chosen = 116, remaining = enabled); }
      enabled -= (116);
    }
    if (117 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 117, remaining = enabled); }
      if ($) { return (chosen = 117, remaining = enabled); }
      enabled -= (117);
    }
    if (118 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 118, remaining = enabled); }
      if ($) { return (chosen = 118, remaining = enabled); }
      enabled -= (118);
    }
    if (119 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 119, remaining = enabled); }
      if ($) { return (chosen = 119, remaining = enabled); }
      enabled -= (119);
    }
    if (120 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 120, remaining = enabled); }
      if ($) { return (chosen = 120, remaining = enabled); }
      enabled -= (120);
    }
    if (121 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 121, remaining = enabled); }
      if ($) { return (chosen = 121, remaining = enabled); }
      enabled -= (121);
    }
    if (122 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 122, remaining = enabled); }
      if ($) { return (chosen = 122, remaining = enabled); }
      enabled -= (122);
    }
    if (123 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 123, remaining = enabled); }
      if ($) { return (chosen = 123, remaining = enabled); }
      enabled -= (123);
    }
    if (124 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 124, remaining = enabled); }
      if ($) { return (chosen = 124, remaining = enabled); }
      enabled -= (124);
    }
    if (125 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 125, remaining = enabled); }
      if ($) { return (chosen = 125, remaining = enabled); }
      enabled -= (125);
    }
    if (126 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 126, remaining = enabled); }
      if ($) { return (chosen = 126, remaining = enabled); }
      enabled -= (126);
    }
    if (127 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 127, remaining = enabled); }
      if ($) { return (chosen = 127, remaining = enabled); }
      enabled -= (127);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_7(selected: int) {
    if (selected == 112) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 113) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 114) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 115) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 116) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 117) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 118) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 119) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 120) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 121) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 122) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 123) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 124) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 125) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 126) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 127) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_8(enabled: set[int]): set[int] {
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (128); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (129); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (130); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (131); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner)) { enabled += (132); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route)))) && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner)) { enabled += (133); }
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
    return enabled;
  }

  fun SelectChunk_8(enabled: set[int]): tSelection {
    if (128 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 128, remaining = enabled); }
      if ($) { return (chosen = 128, remaining = enabled); }
      enabled -= (128);
    }
    if (129 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 129, remaining = enabled); }
      if ($) { return (chosen = 129, remaining = enabled); }
      enabled -= (129);
    }
    if (130 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 130, remaining = enabled); }
      if ($) { return (chosen = 130, remaining = enabled); }
      enabled -= (130);
    }
    if (131 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 131, remaining = enabled); }
      if ($) { return (chosen = 131, remaining = enabled); }
      enabled -= (131);
    }
    if (132 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 132, remaining = enabled); }
      if ($) { return (chosen = 132, remaining = enabled); }
      enabled -= (132);
    }
    if (133 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 133, remaining = enabled); }
      if ($) { return (chosen = 133, remaining = enabled); }
      enabled -= (133);
    }
    if (134 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 134, remaining = enabled); }
      if ($) { return (chosen = 134, remaining = enabled); }
      enabled -= (134);
    }
    if (135 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 135, remaining = enabled); }
      if ($) { return (chosen = 135, remaining = enabled); }
      enabled -= (135);
    }
    if (136 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 136, remaining = enabled); }
      if ($) { return (chosen = 136, remaining = enabled); }
      enabled -= (136);
    }
    if (137 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 137, remaining = enabled); }
      if ($) { return (chosen = 137, remaining = enabled); }
      enabled -= (137);
    }
    if (138 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 138, remaining = enabled); }
      if ($) { return (chosen = 138, remaining = enabled); }
      enabled -= (138);
    }
    if (139 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 139, remaining = enabled); }
      if ($) { return (chosen = 139, remaining = enabled); }
      enabled -= (139);
    }
    if (140 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 140, remaining = enabled); }
      if ($) { return (chosen = 140, remaining = enabled); }
      enabled -= (140);
    }
    if (141 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 141, remaining = enabled); }
      if ($) { return (chosen = 141, remaining = enabled); }
      enabled -= (141);
    }
    if (142 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 142, remaining = enabled); }
      if ($) { return (chosen = 142, remaining = enabled); }
      enabled -= (142);
    }
    if (143 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 143, remaining = enabled); }
      if ($) { return (chosen = 143, remaining = enabled); }
      enabled -= (143);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_8(selected: int) {
    if (selected == 128) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 129) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 130) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 131) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 132) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 133) { Apply_routing_forward_to_matching_DeliveryTask_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1_HistoryShard_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 134) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 135) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 136) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 137) { Apply_routing_handoff_MatchingQueuePartition_0_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 138) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 139) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 140) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 141) { Apply_routing_handoff_MatchingQueuePartition_1_MatchingOwnerGeneration_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 142) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 143) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_9(enabled: set[int]): set[int] {
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (144); }
    if (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (145); }
    if (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (146); }
    if (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (147); }
    if (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (148); }
    if (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && !(HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration) && (state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner && ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) { enabled += (149); }
    if (Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))) { enabled += (150); }
    if (Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))) { enabled += (151); }
    if (Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))) { enabled += (152); }
    if (Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && ((!(DeliveryRouteClass_0 in exists_DeliveryRouteClass) || (!((source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) && (!(DeliveryRouteClass_1 in exists_DeliveryRouteClass) || (!((source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))) { enabled += (153); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (154); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (155); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (156); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass && MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (157); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current)) { enabled += (158); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass && MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)) { enabled += (159); }
    return enabled;
  }

  fun SelectChunk_9(enabled: set[int]): tSelection {
    if (144 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 144, remaining = enabled); }
      if ($) { return (chosen = 144, remaining = enabled); }
      enabled -= (144);
    }
    if (145 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 145, remaining = enabled); }
      if ($) { return (chosen = 145, remaining = enabled); }
      enabled -= (145);
    }
    if (146 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 146, remaining = enabled); }
      if ($) { return (chosen = 146, remaining = enabled); }
      enabled -= (146);
    }
    if (147 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 147, remaining = enabled); }
      if ($) { return (chosen = 147, remaining = enabled); }
      enabled -= (147);
    }
    if (148 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 148, remaining = enabled); }
      if ($) { return (chosen = 148, remaining = enabled); }
      enabled -= (148);
    }
    if (149 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 149, remaining = enabled); }
      if ($) { return (chosen = 149, remaining = enabled); }
      enabled -= (149);
    }
    if (150 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 150, remaining = enabled); }
      if ($) { return (chosen = 150, remaining = enabled); }
      enabled -= (150);
    }
    if (151 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 151, remaining = enabled); }
      if ($) { return (chosen = 151, remaining = enabled); }
      enabled -= (151);
    }
    if (152 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 152, remaining = enabled); }
      if ($) { return (chosen = 152, remaining = enabled); }
      enabled -= (152);
    }
    if (153 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 153, remaining = enabled); }
      if ($) { return (chosen = 153, remaining = enabled); }
      enabled -= (153);
    }
    if (154 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 154, remaining = enabled); }
      if ($) { return (chosen = 154, remaining = enabled); }
      enabled -= (154);
    }
    if (155 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 155, remaining = enabled); }
      if ($) { return (chosen = 155, remaining = enabled); }
      enabled -= (155);
    }
    if (156 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 156, remaining = enabled); }
      if ($) { return (chosen = 156, remaining = enabled); }
      enabled -= (156);
    }
    if (157 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 157, remaining = enabled); }
      if ($) { return (chosen = 157, remaining = enabled); }
      enabled -= (157);
    }
    if (158 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 158, remaining = enabled); }
      if ($) { return (chosen = 158, remaining = enabled); }
      enabled -= (158);
    }
    if (159 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 159, remaining = enabled); }
      if ($) { return (chosen = 159, remaining = enabled); }
      enabled -= (159);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_9(selected: int) {
    if (selected == 144) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 145) { Apply_routing_handoff_history_owner_HistoryShard_0_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 146) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_0_HistoryOwnerGeneration_0(); return; }
    if (selected == 147) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_0_HistoryOwnerGeneration_1(); return; }
    if (selected == 148) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_1_HistoryOwnerGeneration_0(); return; }
    if (selected == 149) { Apply_routing_handoff_history_owner_HistoryShard_1_HistoryOwnerGeneration_1_HistoryOwnerGeneration_1(); return; }
    if (selected == 150) { Apply_routing_register_poller_Poller_0_DeliveryRouteClass_0(); return; }
    if (selected == 151) { Apply_routing_register_poller_Poller_0_DeliveryRouteClass_1(); return; }
    if (selected == 152) { Apply_routing_register_poller_Poller_1_DeliveryRouteClass_0(); return; }
    if (selected == 153) { Apply_routing_register_poller_Poller_1_DeliveryRouteClass_1(); return; }
    if (selected == 154) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 155) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 156) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 157) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 158) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 159) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_10(enabled: set[int]): set[int] {
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
    return enabled;
  }

  fun SelectChunk_10(enabled: set[int]): tSelection {
    if (160 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 160, remaining = enabled); }
      if ($) { return (chosen = 160, remaining = enabled); }
      enabled -= (160);
    }
    if (161 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 161, remaining = enabled); }
      if ($) { return (chosen = 161, remaining = enabled); }
      enabled -= (161);
    }
    if (162 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 162, remaining = enabled); }
      if ($) { return (chosen = 162, remaining = enabled); }
      enabled -= (162);
    }
    if (163 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 163, remaining = enabled); }
      if ($) { return (chosen = 163, remaining = enabled); }
      enabled -= (163);
    }
    if (164 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 164, remaining = enabled); }
      if ($) { return (chosen = 164, remaining = enabled); }
      enabled -= (164);
    }
    if (165 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 165, remaining = enabled); }
      if ($) { return (chosen = 165, remaining = enabled); }
      enabled -= (165);
    }
    if (166 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 166, remaining = enabled); }
      if ($) { return (chosen = 166, remaining = enabled); }
      enabled -= (166);
    }
    if (167 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 167, remaining = enabled); }
      if ($) { return (chosen = 167, remaining = enabled); }
      enabled -= (167);
    }
    if (168 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 168, remaining = enabled); }
      if ($) { return (chosen = 168, remaining = enabled); }
      enabled -= (168);
    }
    if (169 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 169, remaining = enabled); }
      if ($) { return (chosen = 169, remaining = enabled); }
      enabled -= (169);
    }
    if (170 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 170, remaining = enabled); }
      if ($) { return (chosen = 170, remaining = enabled); }
      enabled -= (170);
    }
    if (171 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 171, remaining = enabled); }
      if ($) { return (chosen = 171, remaining = enabled); }
      enabled -= (171);
    }
    if (172 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 172, remaining = enabled); }
      if ($) { return (chosen = 172, remaining = enabled); }
      enabled -= (172);
    }
    if (173 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 173, remaining = enabled); }
      if ($) { return (chosen = 173, remaining = enabled); }
      enabled -= (173);
    }
    if (174 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 174, remaining = enabled); }
      if ($) { return (chosen = 174, remaining = enabled); }
      enabled -= (174);
    }
    if (175 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 175, remaining = enabled); }
      if ($) { return (chosen = 175, remaining = enabled); }
      enabled -= (175);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_10(selected: int) {
    if (selected == 160) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 161) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 162) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 163) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 164) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 165) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 166) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 167) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 168) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 169) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 170) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 171) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 172) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 173) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 174) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 175) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_11(enabled: set[int]): set[int] {
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
    return enabled;
  }

  fun SelectChunk_11(enabled: set[int]): tSelection {
    if (176 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 176, remaining = enabled); }
      if ($) { return (chosen = 176, remaining = enabled); }
      enabled -= (176);
    }
    if (177 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 177, remaining = enabled); }
      if ($) { return (chosen = 177, remaining = enabled); }
      enabled -= (177);
    }
    if (178 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 178, remaining = enabled); }
      if ($) { return (chosen = 178, remaining = enabled); }
      enabled -= (178);
    }
    if (179 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 179, remaining = enabled); }
      if ($) { return (chosen = 179, remaining = enabled); }
      enabled -= (179);
    }
    if (180 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 180, remaining = enabled); }
      if ($) { return (chosen = 180, remaining = enabled); }
      enabled -= (180);
    }
    if (181 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 181, remaining = enabled); }
      if ($) { return (chosen = 181, remaining = enabled); }
      enabled -= (181);
    }
    if (182 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 182, remaining = enabled); }
      if ($) { return (chosen = 182, remaining = enabled); }
      enabled -= (182);
    }
    if (183 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 183, remaining = enabled); }
      if ($) { return (chosen = 183, remaining = enabled); }
      enabled -= (183);
    }
    if (184 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 184, remaining = enabled); }
      if ($) { return (chosen = 184, remaining = enabled); }
      enabled -= (184);
    }
    if (185 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 185, remaining = enabled); }
      if ($) { return (chosen = 185, remaining = enabled); }
      enabled -= (185);
    }
    if (186 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 186, remaining = enabled); }
      if ($) { return (chosen = 186, remaining = enabled); }
      enabled -= (186);
    }
    if (187 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 187, remaining = enabled); }
      if ($) { return (chosen = 187, remaining = enabled); }
      enabled -= (187);
    }
    if (188 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 188, remaining = enabled); }
      if ($) { return (chosen = 188, remaining = enabled); }
      enabled -= (188);
    }
    if (189 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 189, remaining = enabled); }
      if ($) { return (chosen = 189, remaining = enabled); }
      enabled -= (189);
    }
    if (190 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 190, remaining = enabled); }
      if ($) { return (chosen = 190, remaining = enabled); }
      enabled -= (190);
    }
    if (191 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 191, remaining = enabled); }
      if ($) { return (chosen = 191, remaining = enabled); }
      enabled -= (191);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_11(selected: int) {
    if (selected == 176) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 177) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 178) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 179) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 180) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 181) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 182) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 183) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 184) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 185) { Apply_routing_reserve_compatible_DeliveryTask_0_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 186) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 187) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 188) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 189) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 190) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 191) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_12(enabled: set[int]): set[int] {
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
    return enabled;
  }

  fun SelectChunk_12(enabled: set[int]): tSelection {
    if (192 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 192, remaining = enabled); }
      if ($) { return (chosen = 192, remaining = enabled); }
      enabled -= (192);
    }
    if (193 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 193, remaining = enabled); }
      if ($) { return (chosen = 193, remaining = enabled); }
      enabled -= (193);
    }
    if (194 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 194, remaining = enabled); }
      if ($) { return (chosen = 194, remaining = enabled); }
      enabled -= (194);
    }
    if (195 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 195, remaining = enabled); }
      if ($) { return (chosen = 195, remaining = enabled); }
      enabled -= (195);
    }
    if (196 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 196, remaining = enabled); }
      if ($) { return (chosen = 196, remaining = enabled); }
      enabled -= (196);
    }
    if (197 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 197, remaining = enabled); }
      if ($) { return (chosen = 197, remaining = enabled); }
      enabled -= (197);
    }
    if (198 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 198, remaining = enabled); }
      if ($) { return (chosen = 198, remaining = enabled); }
      enabled -= (198);
    }
    if (199 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 199, remaining = enabled); }
      if ($) { return (chosen = 199, remaining = enabled); }
      enabled -= (199);
    }
    if (200 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 200, remaining = enabled); }
      if ($) { return (chosen = 200, remaining = enabled); }
      enabled -= (200);
    }
    if (201 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 201, remaining = enabled); }
      if ($) { return (chosen = 201, remaining = enabled); }
      enabled -= (201);
    }
    if (202 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 202, remaining = enabled); }
      if ($) { return (chosen = 202, remaining = enabled); }
      enabled -= (202);
    }
    if (203 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 203, remaining = enabled); }
      if ($) { return (chosen = 203, remaining = enabled); }
      enabled -= (203);
    }
    if (204 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 204, remaining = enabled); }
      if ($) { return (chosen = 204, remaining = enabled); }
      enabled -= (204);
    }
    if (205 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 205, remaining = enabled); }
      if ($) { return (chosen = 205, remaining = enabled); }
      enabled -= (205);
    }
    if (206 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 206, remaining = enabled); }
      if ($) { return (chosen = 206, remaining = enabled); }
      enabled -= (206);
    }
    if (207 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 207, remaining = enabled); }
      if ($) { return (chosen = 207, remaining = enabled); }
      enabled -= (207);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_12(selected: int) {
    if (selected == 192) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 193) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 194) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 195) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 196) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 197) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 198) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 199) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 200) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 201) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_0_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 202) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 203) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 204) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 205) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 206) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 207) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_13(enabled: set[int]): set[int] {
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
    return enabled;
  }

  fun SelectChunk_13(enabled: set[int]): tSelection {
    if (208 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 208, remaining = enabled); }
      if ($) { return (chosen = 208, remaining = enabled); }
      enabled -= (208);
    }
    if (209 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 209, remaining = enabled); }
      if ($) { return (chosen = 209, remaining = enabled); }
      enabled -= (209);
    }
    if (210 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 210, remaining = enabled); }
      if ($) { return (chosen = 210, remaining = enabled); }
      enabled -= (210);
    }
    if (211 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 211, remaining = enabled); }
      if ($) { return (chosen = 211, remaining = enabled); }
      enabled -= (211);
    }
    if (212 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 212, remaining = enabled); }
      if ($) { return (chosen = 212, remaining = enabled); }
      enabled -= (212);
    }
    if (213 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 213, remaining = enabled); }
      if ($) { return (chosen = 213, remaining = enabled); }
      enabled -= (213);
    }
    if (214 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 214, remaining = enabled); }
      if ($) { return (chosen = 214, remaining = enabled); }
      enabled -= (214);
    }
    if (215 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 215, remaining = enabled); }
      if ($) { return (chosen = 215, remaining = enabled); }
      enabled -= (215);
    }
    if (216 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 216, remaining = enabled); }
      if ($) { return (chosen = 216, remaining = enabled); }
      enabled -= (216);
    }
    if (217 in enabled) {
      if (sizeof(enabled) == 1) { return (chosen = 217, remaining = enabled); }
      if ($) { return (chosen = 217, remaining = enabled); }
      enabled -= (217);
    }
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_13(selected: int) {
    if (selected == 208) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 209) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_0_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 210) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 211) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 212) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 213) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_0_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
    if (selected == 214) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_0(); return; }
    if (selected == 215) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_0_MatchingOwnerGeneration_1(); return; }
    if (selected == 216) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_0(); return; }
    if (selected == 217) { Apply_routing_reserve_compatible_DeliveryTask_1_DeliveryAttempt_1_Poller_1_DeliveryRouteClass_1_MatchingQueuePartition_1_MatchingOwnerGeneration_1(); return; }
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
    CheckProperty_10();
  }

  fun CheckRelation_0() {
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start) || (WorkObligation_0 in exists_WorkObligation && DeliveryAttempt_0 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start) || (WorkObligation_0 in exists_WorkObligation && DeliveryAttempt_1 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start) || (WorkObligation_1 in exists_WorkObligation && DeliveryAttempt_0 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start) || (WorkObligation_1 in exists_WorkObligation && DeliveryAttempt_1 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start && (source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds source cardinality";
    assert !((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start && (source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds source cardinality";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start && (source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds target cardinality";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start && (source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds target cardinality";
  }

  fun CheckRelation_1() {
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_0 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_1 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller) || (DeliveryAttempt_1 in exists_DeliveryAttempt && Poller_0 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller) || (DeliveryAttempt_1 in exists_DeliveryAttempt && Poller_1 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller && (source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller), "relation delivery-attempt-poller exceeds source cardinality";
    assert !((source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller && (source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller), "relation delivery-attempt-poller exceeds source cardinality";
  }

  fun CheckRelation_2() {
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task) || (DeliveryAttempt_0 in exists_DeliveryAttempt && DeliveryTask_0 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task) || (DeliveryAttempt_0 in exists_DeliveryAttempt && DeliveryTask_1 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task) || (DeliveryAttempt_1 in exists_DeliveryAttempt && DeliveryTask_0 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task) || (DeliveryAttempt_1 in exists_DeliveryAttempt && DeliveryTask_1 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task), "relation delivery-attempt-task exceeds source cardinality";
    assert !((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task), "relation delivery-attempt-task exceeds source cardinality";
  }

  fun CheckRelation_3() {
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner) || (MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner) || (MatchingQueuePartition_1 in exists_MatchingQueuePartition && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-partition-owner has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = MatchingQueuePartition_0, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner), "relation delivery-partition-owner exceeds source cardinality";
    assert !((source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_0) in relation_delivery_partition_owner && (source = MatchingQueuePartition_1, target = MatchingOwnerGeneration_1) in relation_delivery_partition_owner), "relation delivery-partition-owner exceeds source cardinality";
  }

  fun CheckRelation_4() {
    assert !((source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route) || (MatchingQueuePartition_0 in exists_MatchingQueuePartition && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
    assert !((source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route) || (MatchingQueuePartition_1 in exists_MatchingQueuePartition && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
    assert !((source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route) || (MatchingQueuePartition_1 in exists_MatchingQueuePartition && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-partition-route has an absent endpoint";
    assert !((source = MatchingQueuePartition_0, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_0, target = DeliveryRouteClass_1) in relation_delivery_partition_route), "relation delivery-partition-route exceeds source cardinality";
    assert !((source = MatchingQueuePartition_1, target = DeliveryRouteClass_0) in relation_delivery_partition_route && (source = MatchingQueuePartition_1, target = DeliveryRouteClass_1) in relation_delivery_partition_route), "relation delivery-partition-route exceeds source cardinality";
  }

  fun CheckRelation_5() {
    assert !((source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route) || (Poller_0 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route) || (Poller_0 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route) || (Poller_1 in exists_Poller && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route) || (Poller_1 in exists_Poller && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-poller-route has an absent endpoint";
    assert !((source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route), "relation delivery-poller-route exceeds source cardinality";
    assert !((source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route), "relation delivery-poller-route exceeds source cardinality";
  }

  fun CheckRelation_6() {
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation) || (DeliveryTask_1 in exists_DeliveryTask && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation) || (DeliveryTask_1 in exists_DeliveryTask && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation delivery-task-history-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && (source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation), "relation delivery-task-history-owner-generation exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && (source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation), "relation delivery-task-history-owner-generation exceeds source cardinality";
  }

  fun CheckRelation_7() {
    assert !((source = DeliveryTask_0, target = HistoryShard_0) in relation_delivery_task_history_shard) || (DeliveryTask_0 in exists_DeliveryTask && HistoryShard_0 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryShard_1) in relation_delivery_task_history_shard) || (DeliveryTask_0 in exists_DeliveryTask && HistoryShard_1 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
    assert !((source = DeliveryTask_1, target = HistoryShard_0) in relation_delivery_task_history_shard) || (DeliveryTask_1 in exists_DeliveryTask && HistoryShard_0 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
    assert !((source = DeliveryTask_1, target = HistoryShard_1) in relation_delivery_task_history_shard) || (DeliveryTask_1 in exists_DeliveryTask && HistoryShard_1 in exists_HistoryShard), "relation delivery-task-history-shard has an absent endpoint";
    assert !((source = DeliveryTask_0, target = HistoryShard_0) in relation_delivery_task_history_shard && (source = DeliveryTask_0, target = HistoryShard_1) in relation_delivery_task_history_shard), "relation delivery-task-history-shard exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = HistoryShard_0) in relation_delivery_task_history_shard && (source = DeliveryTask_1, target = HistoryShard_1) in relation_delivery_task_history_shard), "relation delivery-task-history-shard exceeds source cardinality";
  }

  fun CheckRelation_8() {
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
  }

  fun CheckRelation_9() {
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || (DeliveryTask_0 in exists_DeliveryTask && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation) || (DeliveryTask_1 in exists_DeliveryTask && MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation) || (DeliveryTask_1 in exists_DeliveryTask && MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration), "relation delivery-task-owner-generation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && (source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation), "relation delivery-task-owner-generation exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && (source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation), "relation delivery-task-owner-generation exceeds source cardinality";
  }

  fun CheckRelation_10() {
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
  }

  fun CheckRelation_11() {
    assert !((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_0 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryRouteClass_1 in exists_DeliveryRouteClass), "relation delivery-task-route has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route), "relation delivery-task-route exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route), "relation delivery-task-route exceeds source cardinality";
  }

  fun CheckRelation_12() {
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner) || (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner) || (HistoryShard_0 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner) || (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner) || (HistoryShard_1 in exists_HistoryShard && HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration), "relation history-shard-owner has an absent endpoint";
    assert !((source = HistoryShard_0, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && (source = HistoryShard_0, target = HistoryOwnerGeneration_1) in relation_history_shard_owner), "relation history-shard-owner exceeds source cardinality";
    assert !((source = HistoryShard_1, target = HistoryOwnerGeneration_0) in relation_history_shard_owner && (source = HistoryShard_1, target = HistoryOwnerGeneration_1) in relation_history_shard_owner), "relation history-shard-owner exceeds source cardinality";
  }

  fun CheckProperty_0() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal)))))))))), "property delivery.coarse-retirement-safety failed";
  }

  fun CheckProperty_1() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue)))))), "property delivery.destination-isolation failed";
  }

  fun CheckProperty_2() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))))), "property delivery.failed-start-is-not-accepted failed";
  }

  fun CheckProperty_3() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))))))))), "property delivery.no-phantom-dispatch failed";
  }

  fun CheckProperty_4() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired))))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))))), "property delivery.no-resurrection failed";
  }

  fun CheckProperty_5() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))), "property delivery.no-split-commit failed";
  }

  fun CheckProperty_6() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current)))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_0, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_0, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((MatchingOwnerGeneration_0 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_1, target = MatchingOwnerGeneration_0) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_0] == MatchingOwnerGeneration_state_current))) || (MatchingOwnerGeneration_1 in exists_MatchingOwnerGeneration && (((source = DeliveryTask_1, target = MatchingOwnerGeneration_1) in relation_delivery_task_owner_generation && state_MatchingOwnerGeneration[MatchingOwnerGeneration_1] == MatchingOwnerGeneration_state_current)))) && ((HistoryOwnerGeneration_0 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_1, target = HistoryOwnerGeneration_0) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_0] == HistoryOwnerGeneration_state_current))) || (HistoryOwnerGeneration_1 in exists_HistoryOwnerGeneration && (((source = DeliveryTask_1, target = HistoryOwnerGeneration_1) in relation_delivery_task_history_owner_generation && state_HistoryOwnerGeneration[HistoryOwnerGeneration_1] == HistoryOwnerGeneration_state_current))))))))))))), "property delivery.owner-generation-fencing failed";
  }

  fun CheckProperty_7() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue)))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue))))))), "property delivery.path-equivalence failed";
  }

  fun CheckProperty_8() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))))), "property delivery.retry-preserves-obligation failed";
  }

  fun CheckProperty_9() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((!(DeliveryTask_0 in exists_DeliveryTask) || (((!(Poller_0 in exists_Poller) || ((!(((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))) && (!(Poller_1 in exists_Poller) || ((!(((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((!(Poller_0 in exists_Poller) || ((!(((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))) && (!(Poller_1 in exists_Poller) || ((!(((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || (((!(DeliveryTask_0 in exists_DeliveryTask) || (((!(Poller_0 in exists_Poller) || ((!(((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))) && (!(Poller_1 in exists_Poller) || ((!(((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_0, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((!(Poller_0 in exists_Poller) || ((!(((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_0, target = DeliveryRouteClass_1) in relation_delivery_poller_route)))))))) && (!(Poller_1 in exists_Poller) || ((!(((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller)) || (((DeliveryRouteClass_0 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_0) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_0) in relation_delivery_poller_route))) || (DeliveryRouteClass_1 in exists_DeliveryRouteClass && (((source = DeliveryTask_1, target = DeliveryRouteClass_1) in relation_delivery_task_route && (source = Poller_1, target = DeliveryRouteClass_1) in relation_delivery_poller_route))))))))))))))), "property delivery.routing-isolation failed";
  }

  fun CheckProperty_10() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))))), "property delivery.single-accepted-start failed";
  }

  fun CheckQuiescent() {
    CheckQuiescentProperty_0();
  }

  fun CheckQuiescentProperty_0() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved))) && (!(WorkObligation_1 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved)))), "quiescent property delivery.ambiguous-commit-resolved failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
