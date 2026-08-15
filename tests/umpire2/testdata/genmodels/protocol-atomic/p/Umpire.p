// Generated from the Umpire verification snapshot. Do not edit.

event eStep;
type tSelection = (chosen: int, remaining: set[int]);

enum Activity { Activity_0 }
enum Activity_state { Activity_state_backing_off, Activity_state_canceled, Activity_state_completed, Activity_state_failed, Activity_state_scheduled, Activity_state_started, Activity_state_timed_out, Activity_state_unspecified }
enum Callback { Callback_0 }
enum Callback_state { Callback_state_unobserved }
enum NexusOperation { NexusOperation_0 }
enum NexusOperation_state { NexusOperation_state_backing_off, NexusOperation_state_canceled, NexusOperation_state_failed, NexusOperation_state_rejected, NexusOperation_state_scheduled, NexusOperation_state_started, NexusOperation_state_succeeded, NexusOperation_state_terminated, NexusOperation_state_timed_out, NexusOperation_state_unspecified }
enum TaskQueue { TaskQueue_0 }
enum Workflow { Workflow_0 }
enum Workflow_state { Workflow_state_canceled, Workflow_state_completed, Workflow_state_created, Workflow_state_failed, Workflow_state_started, Workflow_state_terminated, Workflow_state_timed_out }
enum WorkflowRun { WorkflowRun_0 }
enum WorkflowRun_state { WorkflowRun_state_canceled, WorkflowRun_state_completed, WorkflowRun_state_continued_as_new, WorkflowRun_state_created, WorkflowRun_state_failed, WorkflowRun_state_started, WorkflowRun_state_terminated, WorkflowRun_state_timed_out }
enum WorkflowTask { WorkflowTask_0 }
enum WorkflowTask_state { WorkflowTask_state_added, WorkflowTask_state_created, WorkflowTask_state_discarded, WorkflowTask_state_polled, WorkflowTask_state_stored, WorkflowTask_state_terminated }
type relation_activity_nexus_tuple = (source: Activity, target: NexusOperation);
type relation_callback_handler_run_tuple = (source: Callback, target: WorkflowRun);
type relation_callback_operation_tuple = (source: Callback, target: NexusOperation);
type relation_nexus_activity_tuple = (source: NexusOperation, target: Activity);
type relation_workflow_run_successor_tuple = (source: WorkflowRun, target: WorkflowRun);
type relation_workflow_runs_tuple = (source: Workflow, target: WorkflowRun);

machine UmpireWorld {
  var checkerStep: int;
  var exists_Activity: set[Activity];
  var state_Activity: map[Activity, Activity_state];
  var exists_Callback: set[Callback];
  var state_Callback: map[Callback, Callback_state];
  var exists_NexusOperation: set[NexusOperation];
  var state_NexusOperation: map[NexusOperation, NexusOperation_state];
  var exists_TaskQueue: set[TaskQueue];
  var exists_Workflow: set[Workflow];
  var state_Workflow: map[Workflow, Workflow_state];
  var exists_WorkflowRun: set[WorkflowRun];
  var state_WorkflowRun: map[WorkflowRun, WorkflowRun_state];
  var exists_WorkflowTask: set[WorkflowTask];
  var state_WorkflowTask: map[WorkflowTask, WorkflowTask_state];
  var relation_activity_nexus: set[relation_activity_nexus_tuple];
  var relation_callback_handler_run: set[relation_callback_handler_run_tuple];
  var relation_callback_operation: set[relation_callback_operation_tuple];
  var relation_nexus_activity: set[relation_nexus_activity_tuple];
  var relation_workflow_run_successor: set[relation_workflow_run_successor_tuple];
  var relation_workflow_runs: set[relation_workflow_runs_tuple];

  start state Init {
    entry {
      state_Activity[Activity_0] = Activity_state_unspecified;
      state_Callback[Callback_0] = Callback_state_unobserved;
      state_NexusOperation[NexusOperation_0] = NexusOperation_state_unspecified;
      state_Workflow[Workflow_0] = Workflow_state_created;
      state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_created;
      state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_created;
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
  }

  fun EnabledChunk_0(enabled: set[int]): set[int] {
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_backing_off) { enabled += (0); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_backing_off) { enabled += (1); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_backing_off) { enabled += (2); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (3); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (4); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (5); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (6); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (7); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (8); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (9); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (10); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (11); }
    if (!(Activity_0 in exists_Activity) && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (12); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (13); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (14); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (15); }
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
    if (selected == 1) { Apply_Activity_backing_off_schedule_AnyHosting_Activity_0(); return; }
    if (selected == 2) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_0(); return; }
    if (selected == 3) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_0(); return; }
    if (selected == 4) { Apply_Activity_scheduled_fail_AnyHosting_Activity_0(); return; }
    if (selected == 5) { Apply_Activity_scheduled_start_AnyHosting_Activity_0(); return; }
    if (selected == 6) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_0(); return; }
    if (selected == 7) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_0(); return; }
    if (selected == 8) { Apply_Activity_started_cancel_AnyHosting_Activity_0(); return; }
    if (selected == 9) { Apply_Activity_started_complete_AnyHosting_Activity_0(); return; }
    if (selected == 10) { Apply_Activity_started_fail_AnyHosting_Activity_0(); return; }
    if (selected == 11) { Apply_Activity_started_timeout_AnyHosting_Activity_0(); return; }
    if (selected == 12) { Apply_Activity_unspecified_schedule_AnyHosting_Activity_0(); return; }
    if (selected == 13) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_0(); return; }
    if (selected == 14) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_0(); return; }
    if (selected == 15) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_0(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_1(enabled: set[int]): set[int] {
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (16); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (17); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (18); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (19); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (20); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (21); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (22); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (23); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (24); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (25); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (26); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (27); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (28); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (29); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (30); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (31); }
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
    if (selected == 16) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_0(); return; }
    if (selected == 17) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_0(); return; }
    if (selected == 18) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_0(); return; }
    if (selected == 19) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_0(); return; }
    if (selected == 20) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_0(); return; }
    if (selected == 21) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_0(); return; }
    if (selected == 22) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_0(); return; }
    if (selected == 23) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_0(); return; }
    if (selected == 24) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_0(); return; }
    if (selected == 25) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_0(); return; }
    if (selected == 26) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_0(); return; }
    if (selected == 27) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_0(); return; }
    if (selected == 28) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_0(); return; }
    if (selected == 29) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_0(); return; }
    if (selected == 30) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_0(); return; }
    if (selected == 31) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_0(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_2(enabled: set[int]): set[int] {
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (32); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (33); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (34); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (35); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (36); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (37); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (38); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (39); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (40); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (41); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (42); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (43); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (44); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (45); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (46); }
    if (!(Workflow_0 in exists_Workflow) && state_Workflow[Workflow_0] == Workflow_state_created) { enabled += (47); }
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
    if (selected == 32) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_0(); return; }
    if (selected == 33) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_0(); return; }
    if (selected == 34) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_0(); return; }
    if (selected == 35) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_0(); return; }
    if (selected == 36) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_0(); return; }
    if (selected == 37) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_0(); return; }
    if (selected == 38) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_0(); return; }
    if (selected == 39) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_0(); return; }
    if (selected == 40) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_0(); return; }
    if (selected == 41) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_0(); return; }
    if (selected == 42) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_0(); return; }
    if (selected == 43) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_0(); return; }
    if (selected == 44) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_0(); return; }
    if (selected == 45) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_0(); return; }
    if (selected == 46) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_0(); return; }
    if (selected == 47) { Apply_Workflow_created_start_Standalone_Workflow_0(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_3(enabled: set[int]): set[int] {
    if (Workflow_0 in exists_Workflow && state_Workflow[Workflow_0] == Workflow_state_started) { enabled += (48); }
    if (Workflow_0 in exists_Workflow && state_Workflow[Workflow_0] == Workflow_state_started) { enabled += (49); }
    if (Workflow_0 in exists_Workflow && state_Workflow[Workflow_0] == Workflow_state_started) { enabled += (50); }
    if (Workflow_0 in exists_Workflow && state_Workflow[Workflow_0] == Workflow_state_started) { enabled += (51); }
    if (Workflow_0 in exists_Workflow && state_Workflow[Workflow_0] == Workflow_state_started) { enabled += (52); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created) { enabled += (53); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (54); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (55); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (56); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (57); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (58); }
    if (WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (59); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added) { enabled += (60); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added) { enabled += (61); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added) { enabled += (62); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added) { enabled += (63); }
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
    if (selected == 48) { Apply_Workflow_started_cancel_Standalone_Workflow_0(); return; }
    if (selected == 49) { Apply_Workflow_started_complete_Standalone_Workflow_0(); return; }
    if (selected == 50) { Apply_Workflow_started_fail_Standalone_Workflow_0(); return; }
    if (selected == 51) { Apply_Workflow_started_terminate_Standalone_Workflow_0(); return; }
    if (selected == 52) { Apply_Workflow_started_timeout_Standalone_Workflow_0(); return; }
    if (selected == 53) { Apply_WorkflowRun_created_start_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 54) { Apply_WorkflowRun_started_cancel_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 55) { Apply_WorkflowRun_started_complete_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 56) { Apply_WorkflowRun_started_continue_as_new_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 57) { Apply_WorkflowRun_started_fail_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 58) { Apply_WorkflowRun_started_terminate_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 59) { Apply_WorkflowRun_started_timeout_AnyHosting_WorkflowRun_0(); return; }
    if (selected == 60) { Apply_WorkflowTask_added_discard_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 61) { Apply_WorkflowTask_added_poll_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 62) { Apply_WorkflowTask_added_store_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 63) { Apply_WorkflowTask_added_terminate_AnyHosting_WorkflowTask_0(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_4(enabled: set[int]): set[int] {
    if (!(WorkflowTask_0 in exists_WorkflowTask) && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created) { enabled += (64); }
    if (!(WorkflowTask_0 in exists_WorkflowTask) && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_created) { enabled += (65); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored) { enabled += (66); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored) { enabled += (67); }
    if (WorkflowTask_0 in exists_WorkflowTask && state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored) { enabled += (68); }
    if (!(Activity_0 in exists_Activity) && NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (69); }
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
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_4(selected: int) {
    if (selected == 64) { Apply_WorkflowTask_created_add_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 65) { Apply_WorkflowTask_created_terminate_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 66) { Apply_WorkflowTask_stored_discard_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 67) { Apply_WorkflowTask_stored_poll_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 68) { Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_0(); return; }
    if (selected == 69) { Apply_regression_nexus_start_activity_Activity_0_NexusOperation_0(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun Apply_Activity_backing_off_cancel_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.backing_off.cancel.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_backing_off_schedule_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.backing_off.schedule.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_scheduled;
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

  fun Apply_Activity_scheduled_start_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.scheduled.start.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_started;
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

  fun Apply_Activity_unspecified_schedule_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.unspecified.schedule.AnyHosting entity=Activity#0";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_scheduled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.backing_off.schedule.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_scheduled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.backing_off.schedule.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_scheduled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.backing_off.terminate.Embedded entity=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.backing_off.terminate.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.backing_off.timeout.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.backing_off.timeout.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.attempt_failed.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_backing_off;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.attempt_failed.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_backing_off;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.cancel.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.cancel.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.fail.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.fail.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.start.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_started;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.start.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_started;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.succeed.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_succeeded;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.succeed.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_succeeded;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.terminate.Embedded entity=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.terminate.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.timeout.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.scheduled.timeout.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_cancel_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.cancel.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_cancel_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.cancel.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_fail_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.fail.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_fail_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.fail.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_succeed_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.succeed.Embedded op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_succeeded;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_succeed_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.succeed.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_succeeded;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_terminate_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.terminate.Embedded entity=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_terminate_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.terminate.Standalone op=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_timeout_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.timeout.Embedded entity=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_started_timeout_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.started.timeout.Standalone entity=NexusOperation#0";
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.unspecified.reject.Embedded entity=NexusOperation#0";
    exists_NexusOperation += (NexusOperation_0);
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.unspecified.reject.Standalone entity=NexusOperation#0";
    exists_NexusOperation += (NexusOperation_0);
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_rejected;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.unspecified.schedule.Embedded op=NexusOperation#0";
    exists_NexusOperation += (NexusOperation_0);
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_scheduled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_0() {
    print "UMPIRE_ACTION NexusOperation.unspecified.schedule.Standalone op=NexusOperation#0";
    exists_NexusOperation += (NexusOperation_0);
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_scheduled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Workflow_created_start_Standalone_Workflow_0() {
    print "UMPIRE_ACTION Workflow.created.start.Standalone wf=Workflow#0";
    exists_Workflow += (Workflow_0);
    state_Workflow[Workflow_0] = Workflow_state_started;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Workflow_started_cancel_Standalone_Workflow_0() {
    print "UMPIRE_ACTION Workflow.started.cancel.Standalone entity=Workflow#0";
    state_Workflow[Workflow_0] = Workflow_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Workflow_started_complete_Standalone_Workflow_0() {
    print "UMPIRE_ACTION Workflow.started.complete.Standalone wf=Workflow#0";
    state_Workflow[Workflow_0] = Workflow_state_completed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Workflow_started_fail_Standalone_Workflow_0() {
    print "UMPIRE_ACTION Workflow.started.fail.Standalone entity=Workflow#0";
    state_Workflow[Workflow_0] = Workflow_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Workflow_started_terminate_Standalone_Workflow_0() {
    print "UMPIRE_ACTION Workflow.started.terminate.Standalone entity=Workflow#0";
    state_Workflow[Workflow_0] = Workflow_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Workflow_started_timeout_Standalone_Workflow_0() {
    print "UMPIRE_ACTION Workflow.started.timeout.Standalone entity=Workflow#0";
    state_Workflow[Workflow_0] = Workflow_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_WorkflowRun_created_start_AnyHosting_WorkflowRun_0() {
    print "UMPIRE_ACTION WorkflowRun.created.start.AnyHosting entity=WorkflowRun#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
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

  fun Apply_WorkflowTask_added_discard_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.added.discard.AnyHosting entity=WorkflowTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
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

  fun Apply_WorkflowTask_added_store_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.added.store.AnyHosting entity=WorkflowTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_stored;
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

  fun Apply_WorkflowTask_created_add_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.created.add.AnyHosting entity=WorkflowTask#0";
    exists_WorkflowTask += (WorkflowTask_0);
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_added;
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

  fun Apply_WorkflowTask_stored_discard_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.stored.discard.AnyHosting entity=WorkflowTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_discarded;
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

  fun Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_0() {
    print "UMPIRE_ACTION WorkflowTask.stored.terminate.AnyHosting entity=WorkflowTask#0";
    state_WorkflowTask[WorkflowTask_0] = WorkflowTask_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_regression_nexus_start_activity_Activity_0_NexusOperation_0() {
    print "UMPIRE_ACTION regression.nexus.start_activity activity=Activity#0 operation=NexusOperation#0";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_completed;
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_succeeded;
    relation_nexus_activity += ((source = NexusOperation_0, target = Activity_0));
    relation_activity_nexus += ((source = Activity_0, target = NexusOperation_0));
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
    CheckProperty_0();
    CheckProperty_1();
    CheckProperty_2();
  }

  fun CheckRelation_0() {
    assert !((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus) || (Activity_0 in exists_Activity && NexusOperation_0 in exists_NexusOperation), "relation activity-nexus has an absent endpoint";
  }

  fun CheckRelation_1() {
    assert !((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (Callback_0 in exists_Callback && WorkflowRun_0 in exists_WorkflowRun), "relation callback-handler-run has an absent endpoint";
  }

  fun CheckRelation_2() {
    assert !((source = Callback_0, target = NexusOperation_0) in relation_callback_operation) || (Callback_0 in exists_Callback && NexusOperation_0 in exists_NexusOperation), "relation callback-operation has an absent endpoint";
  }

  fun CheckRelation_3() {
    assert !((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity) || (NexusOperation_0 in exists_NexusOperation && Activity_0 in exists_Activity), "relation nexus-activity has an absent endpoint";
  }

  fun CheckRelation_4() {
    assert !((source = WorkflowRun_0, target = WorkflowRun_0) in relation_workflow_run_successor) || (WorkflowRun_0 in exists_WorkflowRun && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-run-successor has an absent endpoint";
  }

  fun CheckRelation_5() {
    assert !((source = Workflow_0, target = WorkflowRun_0) in relation_workflow_runs) || (Workflow_0 in exists_Workflow && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-runs has an absent endpoint";
  }

  fun CheckProperty_0() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity) || ((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus)))))))), "property NexusActivityForwardLinkConsistency failed";
  }

  fun CheckProperty_1() {
    assert ((!(Activity_0 in exists_Activity) || (((!(NexusOperation_0 in exists_NexusOperation) || ((!((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus) || ((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity)))))))), "property NexusActivityReverseLinkConsistency failed";
  }

  fun CheckProperty_2() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity) || ((state_NexusOperation[NexusOperation_0] == NexusOperation_state_succeeded && state_Activity[Activity_0] == Activity_state_completed))))))))), "property NexusActivityTerminalRefinement failed";
  }

  fun CheckQuiescent() {
    CheckQuiescentProperty_0();
    CheckQuiescentProperty_1();
    CheckQuiescentProperty_2();
    CheckQuiescentProperty_3();
    CheckQuiescentProperty_4();
    CheckQuiescentProperty_5();
    CheckQuiescentProperty_6();
  }

  fun CheckQuiescentProperty_0() {
    assert ((!(Activity_0 in exists_Activity) || (!(state_Activity[Activity_0] == Activity_state_backing_off)))), "quiescent property Activity.backing_off.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_1() {
    assert ((!(Activity_0 in exists_Activity) || (!(state_Activity[Activity_0] == Activity_state_scheduled)))), "quiescent property Activity.scheduled.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_2() {
    assert ((!(Activity_0 in exists_Activity) || (!(state_Activity[Activity_0] == Activity_state_started)))), "quiescent property Activity.started.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_3() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off)))), "quiescent property NexusOperation.backing_off.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_4() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled)))), "quiescent property NexusOperation.scheduled.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_5() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_0] == NexusOperation_state_started)))), "quiescent property NexusOperation.started.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_6() {
    assert ((!(Workflow_0 in exists_Workflow) || (!(state_Workflow[Workflow_0] == Workflow_state_started)))), "quiescent property Workflow.started.quiescent-progress failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
