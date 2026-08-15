// Generated from the Umpire verification snapshot. Do not edit.

event eStep;
type tSelection = (chosen: int, remaining: set[int]);

enum Activity { Activity_0 }
enum Activity_state { Activity_state_backing_off, Activity_state_canceled, Activity_state_completed, Activity_state_failed, Activity_state_scheduled, Activity_state_started, Activity_state_timed_out, Activity_state_unspecified }
enum DeliveryAttempt { DeliveryAttempt_0, DeliveryAttempt_1 }
enum DeliveryAttempt_state { DeliveryAttempt_state_accepted, DeliveryAttempt_state_completed, DeliveryAttempt_state_dispatched, DeliveryAttempt_state_failed, DeliveryAttempt_state_rejected, DeliveryAttempt_state_reserved }
enum DeliveryQueue { DeliveryQueue_0, DeliveryQueue_1 }
enum DeliveryQueue_state { DeliveryQueue_state_available }
enum DeliveryTask { DeliveryTask_0, DeliveryTask_1 }
enum DeliveryTask_state { DeliveryTask_state_acknowledged, DeliveryTask_state_authorized, DeliveryTask_state_backlogged, DeliveryTask_state_dispatched, DeliveryTask_state_pending, DeliveryTask_state_reserved, DeliveryTask_state_retired, DeliveryTask_state_sync_offered }
enum Poller { Poller_0, Poller_1 }
enum Poller_state { Poller_state_available }
enum WorkObligation { WorkObligation_0, WorkObligation_1 }
enum WorkObligation_state { WorkObligation_state_accepted, WorkObligation_state_terminal, WorkObligation_state_unresolved, WorkObligation_state_valid }
type relation_activity_delivery_task_tuple = (source: Activity, target: DeliveryTask);
type relation_activity_obligation_tuple = (source: Activity, target: WorkObligation);
type relation_delivery_accepted_start_tuple = (source: WorkObligation, target: DeliveryAttempt);
type relation_delivery_attempt_poller_tuple = (source: DeliveryAttempt, target: Poller);
type relation_delivery_attempt_task_tuple = (source: DeliveryAttempt, target: DeliveryTask);
type relation_delivery_task_obligation_tuple = (source: DeliveryTask, target: WorkObligation);
type relation_delivery_task_queue_tuple = (source: DeliveryTask, target: DeliveryQueue);

machine UmpireWorld {
  var checkerStep: int;
  var exists_Activity: set[Activity];
  var state_Activity: map[Activity, Activity_state];
  var exists_DeliveryAttempt: set[DeliveryAttempt];
  var state_DeliveryAttempt: map[DeliveryAttempt, DeliveryAttempt_state];
  var exists_DeliveryQueue: set[DeliveryQueue];
  var state_DeliveryQueue: map[DeliveryQueue, DeliveryQueue_state];
  var exists_DeliveryTask: set[DeliveryTask];
  var state_DeliveryTask: map[DeliveryTask, DeliveryTask_state];
  var exists_Poller: set[Poller];
  var state_Poller: map[Poller, Poller_state];
  var exists_WorkObligation: set[WorkObligation];
  var state_WorkObligation: map[WorkObligation, WorkObligation_state];
  var relation_activity_delivery_task: set[relation_activity_delivery_task_tuple];
  var relation_activity_obligation: set[relation_activity_obligation_tuple];
  var relation_delivery_accepted_start: set[relation_delivery_accepted_start_tuple];
  var relation_delivery_attempt_poller: set[relation_delivery_attempt_poller_tuple];
  var relation_delivery_attempt_task: set[relation_delivery_attempt_task_tuple];
  var relation_delivery_task_obligation: set[relation_delivery_task_obligation_tuple];
  var relation_delivery_task_queue: set[relation_delivery_task_queue_tuple];

  start state Init {
    entry {
      state_Activity[Activity_0] = Activity_state_unspecified;
      state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
      state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
      state_DeliveryQueue[DeliveryQueue_0] = DeliveryQueue_state_available;
      state_DeliveryQueue[DeliveryQueue_1] = DeliveryQueue_state_available;
      exists_DeliveryQueue += (DeliveryQueue_0);
      exists_DeliveryQueue += (DeliveryQueue_1);
      state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
      state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
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
  }

  fun EnabledChunk_0(enabled: set[int]): set[int] {
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_backing_off) { enabled += (0); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_backing_off) { enabled += (1); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (2); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (3); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (4); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (5); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (6); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (7); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (8); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (9); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (10); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (11); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (12); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (13); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (14); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (15); }
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
    if (selected == 0) { Apply_Activity_backing_off_cancel_AnyHosting_Activity_0(); return; }
    if (selected == 1) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_0(); return; }
    if (selected == 2) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_0(); return; }
    if (selected == 3) { Apply_Activity_scheduled_fail_AnyHosting_Activity_0(); return; }
    if (selected == 4) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_0(); return; }
    if (selected == 5) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_0(); return; }
    if (selected == 6) { Apply_Activity_started_cancel_AnyHosting_Activity_0(); return; }
    if (selected == 7) { Apply_Activity_started_complete_AnyHosting_Activity_0(); return; }
    if (selected == 8) { Apply_Activity_started_fail_AnyHosting_Activity_0(); return; }
    if (selected == 9) { Apply_Activity_started_timeout_AnyHosting_Activity_0(); return; }
    if (selected == 10) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 11) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 12) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 13) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 14) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 15) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_1(enabled: set[int]): set[int] {
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (16); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (17); }
    if (!(Activity_0 in exists_Activity) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (18); }
    if (!(Activity_0 in exists_Activity) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (19); }
    if (!(Activity_0 in exists_Activity) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (20); }
    if (!(Activity_0 in exists_Activity) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (21); }
    if (!(Activity_0 in exists_Activity) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (22); }
    if (!(Activity_0 in exists_Activity) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (23); }
    if (!(Activity_0 in exists_Activity) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (24); }
    if (!(Activity_0 in exists_Activity) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (25); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (26); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (27); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (28); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (29); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (30); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (31); }
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
    if (selected == 16) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 17) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 18) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 19) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 20) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 21) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 22) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 23) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 24) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 25) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 26) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 27) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 28) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 29) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 30) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 31) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_2(enabled: set[int]): set[int] {
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (32); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_Activity[Activity_0] == Activity_state_scheduled && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (33); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (34); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (35); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (36); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (37); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (38); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (39); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (40); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (41); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (42); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (43); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (44); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (45); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (46); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (47); }
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
    if (selected == 32) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 33) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 34) { Apply_activity_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 35) { Apply_activity_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1(); return; }
    if (selected == 36) { Apply_activity_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0(); return; }
    if (selected == 37) { Apply_activity_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1(); return; }
    if (selected == 38) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 39) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 40) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 41) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 42) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 43) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 44) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 45) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 46) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 47) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_3(enabled: set[int]): set[int] {
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (48); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (49); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (50); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (51); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (52); }
    if (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (53); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (54); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (55); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (56); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (57); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (58); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (59); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (60); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) { enabled += (61); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (62); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (63); }
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
    if (selected == 48) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 49) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 50) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 51) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 52) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 53) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 54) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 55) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 56) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 57) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 58) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 59) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 60) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 61) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 62) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 63) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_4(enabled: set[int]): set[int] {
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (64); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (65); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (66); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (67); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (68); }
    if (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_Activity[Activity_0] == Activity_state_backing_off && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task)) { enabled += (69); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (70); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (71); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (72); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (73); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (74); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (75); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (76); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (77); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (78); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (79); }
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
    if (selected == 64) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 65) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 66) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 67) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 68) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 69) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 70) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 71) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 72) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 73) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 74) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 75) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 76) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 77) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 78) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 79) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_5(enabled: set[int]): set[int] {
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (80); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (81); }
    if (DeliveryTask_0 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (82); }
    if (DeliveryTask_1 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (83); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (84); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (85); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (86); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (87); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (88); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (89); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (90); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (91); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (92); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (93); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (94); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (95); }
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
    if (selected == 80) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_0(); return; }
    if (selected == 81) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_1(); return; }
    if (selected == 82) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
    if (selected == 83) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
    if (selected == 84) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 85) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 86) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 87) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 88) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 89) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 90) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 91) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 92) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_0(); return; }
    if (selected == 93) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_1(); return; }
    if (selected == 94) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_0(); return; }
    if (selected == 95) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_6(enabled: set[int]): set[int] {
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (96); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (97); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (98); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (99); }
    if (DeliveryTask_0 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged) { enabled += (100); }
    if (DeliveryTask_1 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged) { enabled += (101); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (102); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (103); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (104); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (105); }
    if (DeliveryTask_0 in exists_DeliveryTask && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (106); }
    if (DeliveryTask_1 in exists_DeliveryTask && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (107); }
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
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_6(selected: int) {
    if (selected == 96) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_0(); return; }
    if (selected == 97) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_1(); return; }
    if (selected == 98) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_0(); return; }
    if (selected == 99) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_1(); return; }
    if (selected == 100) { Apply_delivery_retire_DeliveryTask_0(); return; }
    if (selected == 101) { Apply_delivery_retire_DeliveryTask_1(); return; }
    if (selected == 102) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 103) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 104) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 105) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 106) { Apply_delivery_spool_DeliveryTask_0(); return; }
    if (selected == 107) { Apply_delivery_spool_DeliveryTask_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun Apply_Activity_backing_off_cancel_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.backing_off.cancel.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_backing_off_timeout_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.backing_off.timeout.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_scheduled_cancel_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.scheduled.cancel.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_scheduled_fail_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.scheduled.fail.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_scheduled_timeout_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.scheduled.timeout.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_started_attempt_failed_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.started.attempt_failed.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_backing_off;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_started_cancel_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.started.cancel.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_started_complete_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.started.complete.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_completed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_started_fail_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.started.fail.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_started_timeout_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.started.timeout.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION activity.delivery.authorize entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_Activity[Activity_0] = Activity_state_started;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION activity.delivery.authorize entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_Activity[Activity_0] = Activity_state_started;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION activity.delivery.authorize entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_Activity[Activity_0] = Activity_state_started;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION activity.delivery.authorize entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_Activity[Activity_0] = Activity_state_started;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION activity.delivery.authorize entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_Activity[Activity_0] = Activity_state_started;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION activity.delivery.authorize entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_Activity[Activity_0] = Activity_state_started;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION activity.delivery.authorize entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_Activity[Activity_0] = Activity_state_started;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION activity.delivery.authorize entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_Activity[Activity_0] = Activity_state_started;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.persist entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_scheduled;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.persist entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_scheduled;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.persist entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_scheduled;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.persist entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_scheduled;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.persist entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_scheduled;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.persist entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_scheduled;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.persist entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_scheduled;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.persist entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_scheduled;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION activity.delivery.reject entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION activity.delivery.reject entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION activity.delivery.reject entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION activity.delivery.reject entity=Activity#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION activity.delivery.reject entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION activity.delivery.reject entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION activity.delivery.reject entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION activity.delivery.reject entity=Activity#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0() {
    print "UMPIRE_ACTION activity.delivery.resolve-persisted obligation=WorkObligation#0 task=DeliveryTask#0";
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1() {
    print "UMPIRE_ACTION activity.delivery.resolve-persisted obligation=WorkObligation#0 task=DeliveryTask#1";
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0() {
    print "UMPIRE_ACTION activity.delivery.resolve-persisted obligation=WorkObligation#1 task=DeliveryTask#0";
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1() {
    print "UMPIRE_ACTION activity.delivery.resolve-persisted obligation=WorkObligation#1 task=DeliveryTask#1";
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#0 previous-task=DeliveryTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_0));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_0));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION activity.delivery.retry entity=Activity#0 previous-obligation=WorkObligation#1 previous-task=DeliveryTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    state_Activity[Activity_0] = Activity_state_scheduled;
    relation_activity_obligation -= ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task -= ((source = Activity_0, target = DeliveryTask_1));
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_activity_obligation += ((source = Activity_0, target = WorkObligation_1));
    relation_activity_delivery_task += ((source = Activity_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
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

  fun Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_0() {
    print "UMPIRE_ACTION delivery.reserve task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_1() {
    print "UMPIRE_ACTION delivery.reserve task=DeliveryTask#0 attempt=DeliveryAttempt#0 poller=Poller#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_0() {
    print "UMPIRE_ACTION delivery.reserve task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_1() {
    print "UMPIRE_ACTION delivery.reserve task=DeliveryTask#0 attempt=DeliveryAttempt#1 poller=Poller#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_0));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_0() {
    print "UMPIRE_ACTION delivery.reserve task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_1() {
    print "UMPIRE_ACTION delivery.reserve task=DeliveryTask#1 attempt=DeliveryAttempt#0 poller=Poller#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_0);
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_0, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_0, target = Poller_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_0() {
    print "UMPIRE_ACTION delivery.reserve task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_1() {
    print "UMPIRE_ACTION delivery.reserve task=DeliveryTask#1 attempt=DeliveryAttempt#1 poller=Poller#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_reserved;
    exists_DeliveryAttempt += (DeliveryAttempt_1);
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_reserved;
    relation_delivery_attempt_task += ((source = DeliveryAttempt_1, target = DeliveryTask_1));
    relation_delivery_attempt_poller += ((source = DeliveryAttempt_1, target = Poller_1));
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

  fun CheckSafety() {
    CheckRelation_0();
    CheckRelation_1();
    CheckRelation_2();
    CheckRelation_3();
    CheckRelation_4();
    CheckRelation_5();
    CheckRelation_6();
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
    assert !((source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task) || (Activity_0 in exists_Activity && DeliveryTask_0 in exists_DeliveryTask), "relation activity-delivery-task has an absent endpoint";
    assert !((source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task) || (Activity_0 in exists_Activity && DeliveryTask_1 in exists_DeliveryTask), "relation activity-delivery-task has an absent endpoint";
    assert !((source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task), "relation activity-delivery-task exceeds source cardinality";
  }

  fun CheckRelation_1() {
    assert !((source = Activity_0, target = WorkObligation_0) in relation_activity_obligation) || (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation), "relation activity-obligation has an absent endpoint";
    assert !((source = Activity_0, target = WorkObligation_1) in relation_activity_obligation) || (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation), "relation activity-obligation has an absent endpoint";
    assert !((source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation), "relation activity-obligation exceeds source cardinality";
  }

  fun CheckRelation_2() {
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start) || (WorkObligation_0 in exists_WorkObligation && DeliveryAttempt_0 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start) || (WorkObligation_0 in exists_WorkObligation && DeliveryAttempt_1 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start) || (WorkObligation_1 in exists_WorkObligation && DeliveryAttempt_0 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start) || (WorkObligation_1 in exists_WorkObligation && DeliveryAttempt_1 in exists_DeliveryAttempt), "relation delivery-accepted-start has an absent endpoint";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start && (source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds source cardinality";
    assert !((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start && (source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds source cardinality";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start && (source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds target cardinality";
    assert !((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start && (source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start), "relation delivery-accepted-start exceeds target cardinality";
  }

  fun CheckRelation_3() {
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_0 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller) || (DeliveryAttempt_0 in exists_DeliveryAttempt && Poller_1 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller) || (DeliveryAttempt_1 in exists_DeliveryAttempt && Poller_0 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller) || (DeliveryAttempt_1 in exists_DeliveryAttempt && Poller_1 in exists_Poller), "relation delivery-attempt-poller has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = Poller_0) in relation_delivery_attempt_poller && (source = DeliveryAttempt_0, target = Poller_1) in relation_delivery_attempt_poller), "relation delivery-attempt-poller exceeds source cardinality";
    assert !((source = DeliveryAttempt_1, target = Poller_0) in relation_delivery_attempt_poller && (source = DeliveryAttempt_1, target = Poller_1) in relation_delivery_attempt_poller), "relation delivery-attempt-poller exceeds source cardinality";
  }

  fun CheckRelation_4() {
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task) || (DeliveryAttempt_0 in exists_DeliveryAttempt && DeliveryTask_0 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task) || (DeliveryAttempt_0 in exists_DeliveryAttempt && DeliveryTask_1 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task) || (DeliveryAttempt_1 in exists_DeliveryAttempt && DeliveryTask_0 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task) || (DeliveryAttempt_1 in exists_DeliveryAttempt && DeliveryTask_1 in exists_DeliveryTask), "relation delivery-attempt-task has an absent endpoint";
    assert !((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task), "relation delivery-attempt-task exceeds source cardinality";
    assert !((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task), "relation delivery-attempt-task exceeds source cardinality";
  }

  fun CheckRelation_5() {
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
  }

  fun CheckRelation_6() {
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
  }

  fun CheckProperty_0() {
    assert ((!(Activity_0 in exists_Activity) || ((!(state_Activity[Activity_0] == Activity_state_started) || (((WorkObligation_0 in exists_WorkObligation && (((source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))) || (WorkObligation_1 in exists_WorkObligation && (((source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))))))))), "property activity.delivery.accepted-start-correspondence failed";
  }

  fun CheckProperty_1() {
    assert ((!(Activity_0 in exists_Activity) || ((!((state_Activity[Activity_0] == Activity_state_scheduled || state_Activity[Activity_0] == Activity_state_started || state_Activity[Activity_0] == Activity_state_backing_off)) || ((((WorkObligation_0 in exists_WorkObligation && ((source = Activity_0, target = WorkObligation_0) in relation_activity_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = Activity_0, target = WorkObligation_1) in relation_activity_obligation))) && ((DeliveryTask_0 in exists_DeliveryTask && ((source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task))))))))), "property activity.delivery.intent-correspondence failed";
  }

  fun CheckProperty_2() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal)))))))))), "property delivery.coarse-retirement-safety failed";
  }

  fun CheckProperty_3() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue)))))), "property delivery.destination-isolation failed";
  }

  fun CheckProperty_4() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))))), "property delivery.failed-start-is-not-accepted failed";
  }

  fun CheckProperty_5() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))))))))), "property delivery.no-phantom-dispatch failed";
  }

  fun CheckProperty_6() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired))))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))))), "property delivery.no-resurrection failed";
  }

  fun CheckProperty_7() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))), "property delivery.no-split-commit failed";
  }

  fun CheckProperty_8() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue)))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue))))))), "property delivery.path-equivalence failed";
  }

  fun CheckProperty_9() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))))), "property delivery.retry-preserves-obligation failed";
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
