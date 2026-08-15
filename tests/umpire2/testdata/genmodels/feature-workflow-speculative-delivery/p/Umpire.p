// Generated from the Umpire verification snapshot. Do not edit.

event eStep;
type tSelection = (chosen: int, remaining: set[int]);

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
enum WorkflowRun { WorkflowRun_0 }
enum WorkflowRun_state { WorkflowRun_state_canceled, WorkflowRun_state_completed, WorkflowRun_state_continued_as_new, WorkflowRun_state_created, WorkflowRun_state_failed, WorkflowRun_state_started, WorkflowRun_state_terminated, WorkflowRun_state_timed_out }
enum WorkflowTask { WorkflowTask_0, WorkflowTask_1 }
enum WorkflowTask_state { WorkflowTask_state_added, WorkflowTask_state_created, WorkflowTask_state_discarded, WorkflowTask_state_polled, WorkflowTask_state_stored, WorkflowTask_state_terminated }
type relation_delivery_accepted_start_tuple = (source: WorkObligation, target: DeliveryAttempt);
type relation_delivery_attempt_poller_tuple = (source: DeliveryAttempt, target: Poller);
type relation_delivery_attempt_task_tuple = (source: DeliveryAttempt, target: DeliveryTask);
type relation_delivery_task_obligation_tuple = (source: DeliveryTask, target: WorkObligation);
type relation_delivery_task_queue_tuple = (source: DeliveryTask, target: DeliveryQueue);
type relation_workflow_task_delivery_task_tuple = (source: WorkflowTask, target: DeliveryTask);
type relation_workflow_task_normal_run_tuple = (source: WorkflowTask, target: WorkflowRun);
type relation_workflow_task_obligation_tuple = (source: WorkflowTask, target: WorkObligation);
type relation_workflow_task_speculative_run_tuple = (source: WorkflowTask, target: WorkflowRun);

machine UmpireWorld {
  var checkerStep: int;
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
  var exists_WorkflowRun: set[WorkflowRun];
  var state_WorkflowRun: map[WorkflowRun, WorkflowRun_state];
  var exists_WorkflowTask: set[WorkflowTask];
  var state_WorkflowTask: map[WorkflowTask, WorkflowTask_state];
  var relation_delivery_accepted_start: set[relation_delivery_accepted_start_tuple];
  var relation_delivery_attempt_poller: set[relation_delivery_attempt_poller_tuple];
  var relation_delivery_attempt_task: set[relation_delivery_attempt_task_tuple];
  var relation_delivery_task_obligation: set[relation_delivery_task_obligation_tuple];
  var relation_delivery_task_queue: set[relation_delivery_task_queue_tuple];
  var relation_workflow_task_delivery_task: set[relation_workflow_task_delivery_task_tuple];
  var relation_workflow_task_normal_run: set[relation_workflow_task_normal_run_tuple];
  var relation_workflow_task_obligation: set[relation_workflow_task_obligation_tuple];
  var relation_workflow_task_speculative_run: set[relation_workflow_task_speculative_run_tuple];

  start state Init {
    entry {
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
      state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_created;
      state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_created;
      state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_created;
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
  }

  fun EnabledChunk_0(enabled: set[int]): set[int] {
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (0); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (1); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (2); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (3); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (4); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (5); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added) { enabled += (6); }
    if (WorkflowTask_1 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added) { enabled += (7); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added) { enabled += (8); }
    if (WorkflowTask_1 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added) { enabled += (9); }
    if (!(WorkflowTask_0 in exists_WorkflowTask) && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created) { enabled += (10); }
    if (!(WorkflowTask_1 in exists_WorkflowTask) && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created) { enabled += (11); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored) { enabled += (12); }
    if (WorkflowTask_1 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored) { enabled += (13); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored) { enabled += (14); }
    if (WorkflowTask_1 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored) { enabled += (15); }
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
    if (selected == 0) { Apply_WorkflowRun_started_cancel_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 1) { Apply_WorkflowRun_started_complete_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 2) { Apply_WorkflowRun_started_continue_as_new_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 3) { Apply_WorkflowRun_started_fail_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 4) { Apply_WorkflowRun_started_terminate_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 5) { Apply_WorkflowRun_started_timeout_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 6) { Apply_WorkflowTask_added_poll_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 7) { Apply_WorkflowTask_added_poll_AnyHosting_WorkflowTask_1(); return; }
    if (selected == 8) { Apply_WorkflowTask_added_terminate_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 9) { Apply_WorkflowTask_added_terminate_AnyHosting_WorkflowTask_1(); return; }
    if (selected == 10) { Apply_WorkflowTask_created_terminate_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 11) { Apply_WorkflowTask_created_terminate_AnyHosting_WorkflowTask_1(); return; }
    if (selected == 12) { Apply_WorkflowTask_stored_poll_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 13) { Apply_WorkflowTask_stored_poll_AnyHosting_WorkflowTask_1(); return; }
    if (selected == 14) { Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 15) { Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_1(enabled: set[int]): set[int] {
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (16); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (17); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (18); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (19); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (20); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (21); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (22); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (23); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (24); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (25); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (26); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (27); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (28); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (29); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (30); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (31); }
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
    if (selected == 16) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 17) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 18) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 19) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 20) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 21) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 22) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 23) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 24) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 25) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 26) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 27) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 28) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 29) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 30) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 31) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_2(enabled: set[int]): set[int] {
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (32); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (33); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (34); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (35); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (36); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (37); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (38); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (39); }
    if (DeliveryTask_0 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (40); }
    if (DeliveryTask_1 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (41); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (42); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (43); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (44); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (45); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (46); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (47); }
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
    if (selected == 32) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 33) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 34) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 35) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 36) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 37) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 38) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 39) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 40) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
    if (selected == 41) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
    if (selected == 42) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 43) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 44) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 45) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 46) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 47) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_3(enabled: set[int]): set[int] {
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (48); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (49); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (50); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (51); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (52); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (53); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (54); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (55); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (56); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (57); }
    if (DeliveryTask_0 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged) { enabled += (58); }
    if (DeliveryTask_1 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged) { enabled += (59); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (60); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (61); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (62); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (63); }
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
    if (selected == 48) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 49) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 50) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_0(); return; }
    if (selected == 51) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_1(); return; }
    if (selected == 52) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_0(); return; }
    if (selected == 53) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_1(); return; }
    if (selected == 54) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_0(); return; }
    if (selected == 55) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_1(); return; }
    if (selected == 56) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_0(); return; }
    if (selected == 57) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_1(); return; }
    if (selected == 58) { Apply_delivery_retire_DeliveryTask_0(); return; }
    if (selected == 59) { Apply_delivery_retire_DeliveryTask_1(); return; }
    if (selected == 60) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 61) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 62) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 63) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_4(enabled: set[int]): set[int] {
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (64); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (65); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (66); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (67); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (68); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (69); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (70); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (71); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (72); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (73); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (74); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (75); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (76); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (77); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (78); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (79); }
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
    if (selected == 64) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 65) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 66) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 67) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 68) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 69) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 70) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 71) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 72) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 73) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 74) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 75) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 76) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 77) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 78) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 79) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_5(enabled: set[int]): set[int] {
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (80); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (81); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (82); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (83); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (84); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (85); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (86); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (87); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (88); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (89); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (90); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (91); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (92); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (93); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (94); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (95); }
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
    if (selected == 80) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 81) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 82) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 83) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 84) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 85) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 86) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 87) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 88) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 89) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 90) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 91) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 92) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 93) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 94) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 95) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_6(enabled: set[int]): set[int] {
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (96); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (97); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (98); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (99); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (100); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (101); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (102); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (103); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (104); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (105); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (106); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (107); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (108); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (109); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (110); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (111); }
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
    if (selected == 96) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 97) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 98) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 99) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 100) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 101) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 102) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 103) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 104) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 105) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 106) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 107) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 108) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 109) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 110) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 111) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_7(enabled: set[int]): set[int] {
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (112); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (113); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (114); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (115); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (116); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (117); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (118); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (119); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (120); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (121); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (122); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (123); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (124); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task))) { enabled += (125); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (126); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task))) { enabled += (127); }
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
    if (selected == 112) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 113) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 114) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 115) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 116) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 117) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 118) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 119) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 120) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 121) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 122) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 123) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
    if (selected == 124) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
    if (selected == 125) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
    if (selected == 126) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
    if (selected == 127) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_8(enabled: set[int]): set[int] {
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation))) { enabled += (128); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))) { enabled += (129); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) { enabled += (130); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) { enabled += (131); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation))) { enabled += (132); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))) { enabled += (133); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) { enabled += (134); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) { enabled += (135); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation))) { enabled += (136); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))) { enabled += (137); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) { enabled += (138); }
    if (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) { enabled += (139); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation))) { enabled += (140); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))) { enabled += (141); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) { enabled += (142); }
    if (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) { enabled += (143); }
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
    if (selected == 128) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 129) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1(); return; }
    if (selected == 130) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0(); return; }
    if (selected == 131) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1(); return; }
    if (selected == 132) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 133) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1(); return; }
    if (selected == 134) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0(); return; }
    if (selected == 135) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1(); return; }
    if (selected == 136) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 137) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1(); return; }
    if (selected == 138) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0(); return; }
    if (selected == 139) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1(); return; }
    if (selected == 140) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 141) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1(); return; }
    if (selected == 142) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0(); return; }
    if (selected == 143) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_9(enabled: set[int]): set[int] {
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created)) { enabled += (144); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created)) { enabled += (145); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created)) { enabled += (146); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created)) { enabled += (147); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created)) { enabled += (148); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created)) { enabled += (149); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created)) { enabled += (150); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created)) { enabled += (151); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created)) { enabled += (152); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created)) { enabled += (153); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created)) { enabled += (154); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created)) { enabled += (155); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created)) { enabled += (156); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created)) { enabled += (157); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created)) { enabled += (158); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created)) { enabled += (159); }
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
    if (selected == 144) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 145) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 146) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 147) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 148) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 149) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 150) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 151) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 152) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 153) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 154) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 155) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 156) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 157) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 158) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 159) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_10(enabled: set[int]): set[int] {
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (160); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (161); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (162); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (163); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (164); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (165); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (166); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_0 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (167); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (168); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (169); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (170); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (171); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (172); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (173); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (174); }
    if (WorkflowRun_0 in exists_WorkflowRun && !(WorkflowTask_1 in exists_WorkflowTask) && !(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_created && !(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) { enabled += (175); }
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
    if (selected == 160) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 161) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 162) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 163) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 164) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 165) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 166) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 167) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 168) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 169) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 170) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 171) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
    if (selected == 172) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
    if (selected == 173) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
    if (selected == 174) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
    if (selected == 175) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_11(enabled: set[int]): set[int] {
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (176); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (177); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (178); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (179); }
    if (WorkflowTask_0 in exists_WorkflowTask && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid))))))) { enabled += (180); }
    if (WorkflowTask_0 in exists_WorkflowTask && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid))))))) { enabled += (181); }
    if (WorkflowTask_1 in exists_WorkflowTask && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid))))))) { enabled += (182); }
    if (WorkflowTask_1 in exists_WorkflowTask && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid))))))) { enabled += (183); }
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
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_11(selected: int) {
    if (selected == 176) { Apply_workflow_task_resolve_normal_WorkObligation_0_DeliveryTask_0(); return; }
    if (selected == 177) { Apply_workflow_task_resolve_normal_WorkObligation_0_DeliveryTask_1(); return; }
    if (selected == 178) { Apply_workflow_task_resolve_normal_WorkObligation_1_DeliveryTask_0(); return; }
    if (selected == 179) { Apply_workflow_task_resolve_normal_WorkObligation_1_DeliveryTask_1(); return; }
    if (selected == 180) { Apply_workflow_task_speculative_fallback_WorkflowTask_0_DeliveryTask_0(); return; }
    if (selected == 181) { Apply_workflow_task_speculative_fallback_WorkflowTask_0_DeliveryTask_1(); return; }
    if (selected == 182) { Apply_workflow_task_speculative_fallback_WorkflowTask_1_DeliveryTask_0(); return; }
    if (selected == 183) { Apply_workflow_task_speculative_fallback_WorkflowTask_1_DeliveryTask_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun Apply_WorkflowRun_started_cancel_AnyHosting_WorkflowRun_0() {
    print "UMPIRE_ACTION WorkflowRun.started.cancel.AnyHosting entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowRun_started_complete_AnyHosting_WorkflowRun_0() {
    print "UMPIRE_ACTION WorkflowRun.started.complete.AnyHosting entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_completed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowRun_started_continue_as_new_AnyHosting_WorkflowRun_0() {
    print "UMPIRE_ACTION WorkflowRun.started.continue_as_new.AnyHosting entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_continued_as_new;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowRun_started_fail_AnyHosting_WorkflowRun_0() {
    print "UMPIRE_ACTION WorkflowRun.started.fail.AnyHosting entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowRun_started_terminate_AnyHosting_WorkflowRun_0() {
    print "UMPIRE_ACTION WorkflowRun.started.terminate.AnyHosting entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowRun_started_timeout_AnyHosting_WorkflowRun_0() {
    print "UMPIRE_ACTION WorkflowRun.started.timeout.AnyHosting entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_added_poll_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.added.poll.AnyHosting entity=WorkflowTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_added_poll_AnyHosting_WorkflowTask_1() {
    print "UMPIRE_ACTION WorkflowTask.added.poll.AnyHosting entity=WorkflowTask#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_added_terminate_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.added.terminate.AnyHosting entity=WorkflowTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_added_terminate_AnyHosting_WorkflowTask_1() {
    print "UMPIRE_ACTION WorkflowTask.added.terminate.AnyHosting entity=WorkflowTask#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_created_terminate_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.created.terminate.AnyHosting entity=WorkflowTask#0";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_created_terminate_AnyHosting_WorkflowTask_1() {
    print "UMPIRE_ACTION WorkflowTask.created.terminate.AnyHosting entity=WorkflowTask#1";
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_stored_poll_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.stored.poll.AnyHosting entity=WorkflowTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_stored_poll_AnyHosting_WorkflowTask_1() {
    print "UMPIRE_ACTION WorkflowTask.stored.poll.AnyHosting entity=WorkflowTask#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.stored.terminate.AnyHosting entity=WorkflowTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_1() {
    print "UMPIRE_ACTION WorkflowTask.stored.terminate.AnyHosting entity=WorkflowTask#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_terminated;
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

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_0, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.authorize-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_polled;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_authorized;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_accepted;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_accepted;
    relation_delivery_accepted_start += ((source = WorkObligation_1, target = DeliveryAttempt_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#0";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_0] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1() {
    print "UMPIRE_ACTION workflow.delivery.reject-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 attempt=DeliveryAttempt#1";
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    state_DeliveryAttempt[DeliveryAttempt_1] = DeliveryAttempt_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-added entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-added entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-added entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-added entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-stored entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-stored entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-stored entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.cancel-speculative-stored entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_discarded;
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_terminal;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_retired;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_0));
    relation_workflow_task_normal_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_0));
    relation_workflow_task_normal_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_1));
    relation_workflow_task_normal_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_1));
    relation_workflow_task_normal_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_0));
    relation_workflow_task_normal_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_0));
    relation_workflow_task_normal_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_1));
    relation_workflow_task_normal_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_1));
    relation_workflow_task_normal_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_0));
    relation_workflow_task_normal_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_0));
    relation_workflow_task_normal_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_1));
    relation_workflow_task_normal_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_1));
    relation_workflow_task_normal_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_0));
    relation_workflow_task_normal_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_0));
    relation_workflow_task_normal_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_1));
    relation_workflow_task_normal_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-normal run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_pending;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_1));
    relation_workflow_task_normal_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_0));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_0));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_1));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_1));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_0));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_0));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_1));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#0 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_0, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_0, target = DeliveryTask_1));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_0));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_0));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_1));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#0 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_0);
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_0));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_0));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_1));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#0";
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_0));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#0 queue=DeliveryQueue#1";
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_0);
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_0, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_0, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_0));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#0";
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_0));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_1));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1() {
    print "UMPIRE_ACTION workflow.task.create-speculative-direct run=WorkflowRun#0 entity=WorkflowTask#1 obligation=WorkObligation#1 task=DeliveryTask#1 queue=DeliveryQueue#1";
    exists_WorkflowTask += (WorkflowTask_1);
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_added;
    exists_WorkObligation += (WorkObligation_1);
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    exists_DeliveryTask += (DeliveryTask_1);
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_sync_offered;
    relation_delivery_task_obligation += ((source = DeliveryTask_1, target = WorkObligation_1));
    relation_delivery_task_queue += ((source = DeliveryTask_1, target = DeliveryQueue_1));
    relation_workflow_task_obligation += ((source = WorkflowTask_1, target = WorkObligation_1));
    relation_workflow_task_delivery_task += ((source = WorkflowTask_1, target = DeliveryTask_1));
    relation_workflow_task_speculative_run += ((source = WorkflowTask_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_resolve_normal_WorkObligation_0_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.resolve-normal obligation=WorkObligation#0 task=DeliveryTask#0";
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_resolve_normal_WorkObligation_0_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.resolve-normal obligation=WorkObligation#0 task=DeliveryTask#1";
    state_WorkObligation[WorkObligation_0] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_resolve_normal_WorkObligation_1_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.resolve-normal obligation=WorkObligation#1 task=DeliveryTask#0";
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_resolve_normal_WorkObligation_1_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.resolve-normal obligation=WorkObligation#1 task=DeliveryTask#1";
    state_WorkObligation[WorkObligation_1] = WorkObligation_state_valid;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_speculative_fallback_WorkflowTask_0_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.speculative-fallback entity=WorkflowTask#0 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_stored;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_speculative_fallback_WorkflowTask_0_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.speculative-fallback entity=WorkflowTask#0 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_stored;
    state_DeliveryTask[DeliveryTask_1] = DeliveryTask_state_backlogged;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_speculative_fallback_WorkflowTask_1_DeliveryTask_0() {
    print "UMPIRE_ACTION workflow.task.speculative-fallback entity=WorkflowTask#1 task=DeliveryTask#0";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_stored;
    state_DeliveryTask[DeliveryTask_0] = DeliveryTask_state_backlogged;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_workflow_task_speculative_fallback_WorkflowTask_1_DeliveryTask_1() {
    print "UMPIRE_ACTION workflow.task.speculative-fallback entity=WorkflowTask#1 task=DeliveryTask#1";
    state_WorkflowTask[WorkflowTask_1] = WorkflowTask_state_stored;
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
    CheckRelation_7();
    CheckRelation_8();
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
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
  }

  fun CheckRelation_4() {
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
  }

  fun CheckRelation_5() {
    assert !((source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task) || (WorkflowTask_0 in exists_WorkflowTask && DeliveryTask_0 in exists_DeliveryTask), "relation workflow-task-delivery-task has an absent endpoint";
    assert !((source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task) || (WorkflowTask_0 in exists_WorkflowTask && DeliveryTask_1 in exists_DeliveryTask), "relation workflow-task-delivery-task has an absent endpoint";
    assert !((source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task) || (WorkflowTask_1 in exists_WorkflowTask && DeliveryTask_0 in exists_DeliveryTask), "relation workflow-task-delivery-task has an absent endpoint";
    assert !((source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task) || (WorkflowTask_1 in exists_WorkflowTask && DeliveryTask_1 in exists_DeliveryTask), "relation workflow-task-delivery-task has an absent endpoint";
    assert !((source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task), "relation workflow-task-delivery-task exceeds source cardinality";
    assert !((source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task), "relation workflow-task-delivery-task exceeds source cardinality";
    assert !((source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task), "relation workflow-task-delivery-task exceeds target cardinality";
    assert !((source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task), "relation workflow-task-delivery-task exceeds target cardinality";
  }

  fun CheckRelation_6() {
    assert !((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run) || (WorkflowTask_0 in exists_WorkflowTask && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-task-normal-run has an absent endpoint";
    assert !((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run) || (WorkflowTask_1 in exists_WorkflowTask && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-task-normal-run has an absent endpoint";
  }

  fun CheckRelation_7() {
    assert !((source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation) || (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation), "relation workflow-task-obligation has an absent endpoint";
    assert !((source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation) || (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation), "relation workflow-task-obligation has an absent endpoint";
    assert !((source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation) || (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation), "relation workflow-task-obligation has an absent endpoint";
    assert !((source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation) || (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation), "relation workflow-task-obligation has an absent endpoint";
    assert !((source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation), "relation workflow-task-obligation exceeds source cardinality";
    assert !((source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation), "relation workflow-task-obligation exceeds source cardinality";
    assert !((source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation), "relation workflow-task-obligation exceeds target cardinality";
    assert !((source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation), "relation workflow-task-obligation exceeds target cardinality";
  }

  fun CheckRelation_8() {
    assert !((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_speculative_run) || (WorkflowTask_0 in exists_WorkflowTask && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-task-speculative-run has an absent endpoint";
    assert !((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_speculative_run) || (WorkflowTask_1 in exists_WorkflowTask && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-task-speculative-run has an absent endpoint";
  }

  fun CheckProperty_0() {
    assert ((!(WorkflowRun_0 in exists_WorkflowRun) || (!(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored) && ((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_speculative_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_speculative_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored) && ((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_speculative_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_speculative_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored))))))))))))), "property SpeculativeTaskCreation failed";
  }

  fun CheckProperty_1() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal)))))))))), "property delivery.coarse-retirement-safety failed";
  }

  fun CheckProperty_2() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue)))))), "property delivery.destination-isolation failed";
  }

  fun CheckProperty_3() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))))), "property delivery.failed-start-is-not-accepted failed";
  }

  fun CheckProperty_4() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))))))))), "property delivery.no-phantom-dispatch failed";
  }

  fun CheckProperty_5() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired))))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))))), "property delivery.no-resurrection failed";
  }

  fun CheckProperty_6() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))), "property delivery.no-split-commit failed";
  }

  fun CheckProperty_7() {
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue)))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue))))))), "property delivery.path-equivalence failed";
  }

  fun CheckProperty_8() {
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))))), "property delivery.retry-preserves-obligation failed";
  }

  fun CheckProperty_9() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))))), "property delivery.single-accepted-start failed";
  }

  fun CheckProperty_10() {
    assert ((!(WorkflowTask_0 in exists_WorkflowTask) || ((!(state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_polled) || (((WorkObligation_0 in exists_WorkObligation && (((source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))) || (WorkObligation_1 in exists_WorkObligation && (((source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))))))) && (!(WorkflowTask_1 in exists_WorkflowTask) || ((!(state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_polled) || (((WorkObligation_0 in exists_WorkObligation && (((source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))) || (WorkObligation_1 in exists_WorkObligation && (((source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))))))))), "property workflow.delivery.accepted-start-correspondence failed";
  }

  fun CheckQuiescent() {
    CheckQuiescentProperty_0();
  }

  fun CheckQuiescentProperty_0() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved))) && (!(WorkObligation_1 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved)))), "quiescent property delivery.ambiguous-commit-resolved failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
