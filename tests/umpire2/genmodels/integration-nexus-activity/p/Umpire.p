// Generated from the Umpire verification snapshot. Do not edit.

event eStep;
type tSelection = (chosen: int, remaining: set[int]);

enum Activity { Activity_0, Activity_1 }
enum Activity_state { Activity_state_backing_off, Activity_state_canceled, Activity_state_completed, Activity_state_failed, Activity_state_scheduled, Activity_state_started, Activity_state_timed_out, Activity_state_unspecified }
enum NexusOperation { NexusOperation_0, NexusOperation_1 }
enum NexusOperation_state { NexusOperation_state_backing_off, NexusOperation_state_canceled, NexusOperation_state_failed, NexusOperation_state_rejected, NexusOperation_state_scheduled, NexusOperation_state_started, NexusOperation_state_succeeded, NexusOperation_state_terminated, NexusOperation_state_timed_out, NexusOperation_state_unspecified }
type relation_activity_nexus_tuple = (source: Activity, target: NexusOperation);
type relation_nexus_activity_tuple = (source: NexusOperation, target: Activity);

machine UmpireWorld {
  var checkerStep: int;
  var exists_Activity: set[Activity];
  var state_Activity: map[Activity, Activity_state];
  var exists_NexusOperation: set[NexusOperation];
  var state_NexusOperation: map[NexusOperation, NexusOperation_state];
  var relation_activity_nexus: set[relation_activity_nexus_tuple];
  var relation_nexus_activity: set[relation_nexus_activity_tuple];

  start state Init {
    entry {
      state_Activity[Activity_0] = Activity_state_unspecified;
      state_Activity[Activity_1] = Activity_state_unspecified;
      state_NexusOperation[NexusOperation_0] = NexusOperation_state_unspecified;
      state_NexusOperation[NexusOperation_1] = NexusOperation_state_unspecified;
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
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_backing_off) { enabled += (1); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_backing_off) { enabled += (2); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_backing_off) { enabled += (3); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_backing_off) { enabled += (4); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_backing_off) { enabled += (5); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (6); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_scheduled) { enabled += (7); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (8); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_scheduled) { enabled += (9); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (10); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_scheduled) { enabled += (11); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_scheduled) { enabled += (12); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_scheduled) { enabled += (13); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (14); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_started) { enabled += (15); }
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
    if (selected == 1) { Apply_Activity_backing_off_cancel_AnyHosting_Activity_1(); return; }
    if (selected == 2) { Apply_Activity_backing_off_schedule_AnyHosting_Activity_0(); return; }
    if (selected == 3) { Apply_Activity_backing_off_schedule_AnyHosting_Activity_1(); return; }
    if (selected == 4) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_0(); return; }
    if (selected == 5) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_1(); return; }
    if (selected == 6) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_0(); return; }
    if (selected == 7) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_1(); return; }
    if (selected == 8) { Apply_Activity_scheduled_fail_AnyHosting_Activity_0(); return; }
    if (selected == 9) { Apply_Activity_scheduled_fail_AnyHosting_Activity_1(); return; }
    if (selected == 10) { Apply_Activity_scheduled_start_AnyHosting_Activity_0(); return; }
    if (selected == 11) { Apply_Activity_scheduled_start_AnyHosting_Activity_1(); return; }
    if (selected == 12) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_0(); return; }
    if (selected == 13) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_1(); return; }
    if (selected == 14) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_0(); return; }
    if (selected == 15) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_1(enabled: set[int]): set[int] {
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (16); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_started) { enabled += (17); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (18); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_started) { enabled += (19); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (20); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_started) { enabled += (21); }
    if (Activity_0 in exists_Activity && state_Activity[Activity_0] == Activity_state_started) { enabled += (22); }
    if (Activity_1 in exists_Activity && state_Activity[Activity_1] == Activity_state_started) { enabled += (23); }
    if (!(Activity_0 in exists_Activity) && state_Activity[Activity_0] == Activity_state_unspecified) { enabled += (24); }
    if (!(Activity_1 in exists_Activity) && state_Activity[Activity_1] == Activity_state_unspecified) { enabled += (25); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (26); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (27); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (28); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (29); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (30); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (31); }
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
    if (selected == 16) { Apply_Activity_started_cancel_AnyHosting_Activity_0(); return; }
    if (selected == 17) { Apply_Activity_started_cancel_AnyHosting_Activity_1(); return; }
    if (selected == 18) { Apply_Activity_started_complete_AnyHosting_Activity_0(); return; }
    if (selected == 19) { Apply_Activity_started_complete_AnyHosting_Activity_1(); return; }
    if (selected == 20) { Apply_Activity_started_fail_AnyHosting_Activity_0(); return; }
    if (selected == 21) { Apply_Activity_started_fail_AnyHosting_Activity_1(); return; }
    if (selected == 22) { Apply_Activity_started_timeout_AnyHosting_Activity_0(); return; }
    if (selected == 23) { Apply_Activity_started_timeout_AnyHosting_Activity_1(); return; }
    if (selected == 24) { Apply_Activity_unspecified_schedule_AnyHosting_Activity_0(); return; }
    if (selected == 25) { Apply_Activity_unspecified_schedule_AnyHosting_Activity_1(); return; }
    if (selected == 26) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_0(); return; }
    if (selected == 27) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_1(); return; }
    if (selected == 28) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_0(); return; }
    if (selected == 29) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_1(); return; }
    if (selected == 30) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_0(); return; }
    if (selected == 31) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_2(enabled: set[int]): set[int] {
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (32); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (33); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (34); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (35); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (36); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (37); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (38); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (39); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (40); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (41); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (42); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (43); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (44); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (45); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (46); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (47); }
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
    if (selected == 32) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_0(); return; }
    if (selected == 33) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_1(); return; }
    if (selected == 34) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_0(); return; }
    if (selected == 35) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_1(); return; }
    if (selected == 36) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_0(); return; }
    if (selected == 37) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_1(); return; }
    if (selected == 38) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_0(); return; }
    if (selected == 39) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_1(); return; }
    if (selected == 40) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_0(); return; }
    if (selected == 41) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_1(); return; }
    if (selected == 42) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_0(); return; }
    if (selected == 43) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_1(); return; }
    if (selected == 44) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_0(); return; }
    if (selected == 45) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_1(); return; }
    if (selected == 46) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_0(); return; }
    if (selected == 47) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_3(enabled: set[int]): set[int] {
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (48); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (49); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (50); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (51); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (52); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (53); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (54); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (55); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (56); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (57); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (58); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (59); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (60); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (61); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (62); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (63); }
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
    if (selected == 48) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_0(); return; }
    if (selected == 49) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_1(); return; }
    if (selected == 50) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_0(); return; }
    if (selected == 51) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_1(); return; }
    if (selected == 52) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_0(); return; }
    if (selected == 53) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_1(); return; }
    if (selected == 54) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_0(); return; }
    if (selected == 55) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_1(); return; }
    if (selected == 56) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_0(); return; }
    if (selected == 57) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_1(); return; }
    if (selected == 58) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_0(); return; }
    if (selected == 59) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_1(); return; }
    if (selected == 60) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_0(); return; }
    if (selected == 61) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_1(); return; }
    if (selected == 62) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_0(); return; }
    if (selected == 63) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_4(enabled: set[int]): set[int] {
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (64); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (65); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (66); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (67); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (68); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (69); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (70); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (71); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (72); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (73); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (74); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (75); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (76); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (77); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (78); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (79); }
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
    if (selected == 64) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_0(); return; }
    if (selected == 65) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_1(); return; }
    if (selected == 66) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_0(); return; }
    if (selected == 67) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_1(); return; }
    if (selected == 68) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_0(); return; }
    if (selected == 69) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_1(); return; }
    if (selected == 70) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_0(); return; }
    if (selected == 71) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_1(); return; }
    if (selected == 72) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_0(); return; }
    if (selected == 73) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_1(); return; }
    if (selected == 74) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_0(); return; }
    if (selected == 75) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_1(); return; }
    if (selected == 76) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_0(); return; }
    if (selected == 77) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_1(); return; }
    if (selected == 78) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_0(); return; }
    if (selected == 79) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_5(enabled: set[int]): set[int] {
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (80); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (81); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (82); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (83); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (84); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (85); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (86); }
    if (!(NexusOperation_1 in exists_NexusOperation) && state_NexusOperation[NexusOperation_1] == NexusOperation_state_unspecified) { enabled += (87); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (88); }
    if (!(NexusOperation_1 in exists_NexusOperation) && state_NexusOperation[NexusOperation_1] == NexusOperation_state_unspecified) { enabled += (89); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (90); }
    if (!(NexusOperation_1 in exists_NexusOperation) && state_NexusOperation[NexusOperation_1] == NexusOperation_state_unspecified) { enabled += (91); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (92); }
    if (!(NexusOperation_1 in exists_NexusOperation) && state_NexusOperation[NexusOperation_1] == NexusOperation_state_unspecified) { enabled += (93); }
    if (!(Activity_0 in exists_Activity) && NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (94); }
    if (!(Activity_0 in exists_Activity) && NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (95); }
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
    if (selected == 80) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_0(); return; }
    if (selected == 81) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_1(); return; }
    if (selected == 82) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_0(); return; }
    if (selected == 83) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_1(); return; }
    if (selected == 84) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_0(); return; }
    if (selected == 85) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_1(); return; }
    if (selected == 86) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_0(); return; }
    if (selected == 87) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_1(); return; }
    if (selected == 88) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_0(); return; }
    if (selected == 89) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_1(); return; }
    if (selected == 90) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_0(); return; }
    if (selected == 91) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_1(); return; }
    if (selected == 92) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_0(); return; }
    if (selected == 93) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_1(); return; }
    if (selected == 94) { Apply_regression_nexus_start_activity_Activity_0_NexusOperation_0(); return; }
    if (selected == 95) { Apply_regression_nexus_start_activity_Activity_0_NexusOperation_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun EnabledChunk_6(enabled: set[int]): set[int] {
    if (!(Activity_1 in exists_Activity) && NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (96); }
    if (!(Activity_1 in exists_Activity) && NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (97); }
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
    return (chosen = -1, remaining = enabled);
  }

  fun ApplyChunk_6(selected: int) {
    if (selected == 96) { Apply_regression_nexus_start_activity_Activity_1_NexusOperation_0(); return; }
    if (selected == 97) { Apply_regression_nexus_start_activity_Activity_1_NexusOperation_1(); return; }
    assert false, "selected candidate is outside its generated chunk";
  }

  fun Apply_Activity_backing_off_cancel_AnyHosting_Activity_0() {
    print "UMPIRE_ACTION Activity.backing_off.cancel.AnyHosting entity=Activity#0";
    state_Activity[Activity_0] = Activity_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_Activity_backing_off_cancel_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.backing_off.cancel.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_canceled;
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

  fun Apply_Activity_backing_off_schedule_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.backing_off.schedule.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_scheduled;
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

  fun Apply_Activity_backing_off_timeout_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.backing_off.timeout.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_timed_out;
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

  fun Apply_Activity_scheduled_cancel_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.scheduled.cancel.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_canceled;
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

  fun Apply_Activity_scheduled_fail_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.scheduled.fail.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_failed;
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

  fun Apply_Activity_scheduled_start_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.scheduled.start.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_started;
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

  fun Apply_Activity_scheduled_timeout_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.scheduled.timeout.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_timed_out;
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

  fun Apply_Activity_started_attempt_failed_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.started.attempt_failed.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_backing_off;
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

  fun Apply_Activity_started_cancel_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.started.cancel.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_canceled;
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

  fun Apply_Activity_started_complete_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.started.complete.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_completed;
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

  fun Apply_Activity_started_fail_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.started.fail.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_failed;
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

  fun Apply_Activity_started_timeout_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.started.timeout.AnyHosting entity=Activity#1";
    state_Activity[Activity_1] = Activity_state_timed_out;
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

  fun Apply_Activity_unspecified_schedule_AnyHosting_Activity_1() {
    print "UMPIRE_ACTION Activity.unspecified.schedule.AnyHosting entity=Activity#1";
    exists_Activity += (Activity_1);
    state_Activity[Activity_1] = Activity_state_scheduled;
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

  fun Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.backing_off.schedule.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_scheduled;
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

  fun Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.backing_off.schedule.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_scheduled;
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

  fun Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.backing_off.terminate.Embedded entity=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_terminated;
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

  fun Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.backing_off.terminate.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_terminated;
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

  fun Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.backing_off.timeout.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_timed_out;
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

  fun Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.backing_off.timeout.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_timed_out;
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

  fun Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.attempt_failed.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_backing_off;
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

  fun Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.attempt_failed.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_backing_off;
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

  fun Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.cancel.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_canceled;
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

  fun Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.cancel.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_canceled;
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

  fun Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.fail.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_failed;
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

  fun Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.fail.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_failed;
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

  fun Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.start.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_started;
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

  fun Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.start.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_started;
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

  fun Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.succeed.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_succeeded;
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

  fun Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.succeed.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_succeeded;
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

  fun Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.terminate.Embedded entity=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_terminated;
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

  fun Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.terminate.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_terminated;
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

  fun Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.timeout.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_timed_out;
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

  fun Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.scheduled.timeout.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_timed_out;
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

  fun Apply_NexusOperation_started_cancel_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.cancel.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_canceled;
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

  fun Apply_NexusOperation_started_cancel_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.cancel.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_canceled;
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

  fun Apply_NexusOperation_started_fail_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.fail.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_failed;
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

  fun Apply_NexusOperation_started_fail_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.fail.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_failed;
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

  fun Apply_NexusOperation_started_succeed_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.succeed.Embedded op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_succeeded;
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

  fun Apply_NexusOperation_started_succeed_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.succeed.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_succeeded;
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

  fun Apply_NexusOperation_started_terminate_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.terminate.Embedded entity=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_terminated;
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

  fun Apply_NexusOperation_started_terminate_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.terminate.Standalone op=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_terminated;
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

  fun Apply_NexusOperation_started_timeout_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.timeout.Embedded entity=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_timed_out;
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

  fun Apply_NexusOperation_started_timeout_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.started.timeout.Standalone entity=NexusOperation#1";
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_timed_out;
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

  fun Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.unspecified.reject.Embedded entity=NexusOperation#1";
    exists_NexusOperation += (NexusOperation_1);
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_rejected;
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

  fun Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.unspecified.reject.Standalone entity=NexusOperation#1";
    exists_NexusOperation += (NexusOperation_1);
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_rejected;
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

  fun Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.unspecified.schedule.Embedded op=NexusOperation#1";
    exists_NexusOperation += (NexusOperation_1);
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_scheduled;
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

  fun Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_1() {
    print "UMPIRE_ACTION NexusOperation.unspecified.schedule.Standalone op=NexusOperation#1";
    exists_NexusOperation += (NexusOperation_1);
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_scheduled;
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

  fun Apply_regression_nexus_start_activity_Activity_0_NexusOperation_1() {
    print "UMPIRE_ACTION regression.nexus.start_activity activity=Activity#0 operation=NexusOperation#1";
    exists_Activity += (Activity_0);
    state_Activity[Activity_0] = Activity_state_completed;
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_succeeded;
    relation_nexus_activity += ((source = NexusOperation_1, target = Activity_0));
    relation_activity_nexus += ((source = Activity_0, target = NexusOperation_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_regression_nexus_start_activity_Activity_1_NexusOperation_0() {
    print "UMPIRE_ACTION regression.nexus.start_activity activity=Activity#1 operation=NexusOperation#0";
    exists_Activity += (Activity_1);
    state_Activity[Activity_1] = Activity_state_completed;
    state_NexusOperation[NexusOperation_0] = NexusOperation_state_succeeded;
    relation_nexus_activity += ((source = NexusOperation_0, target = Activity_1));
    relation_activity_nexus += ((source = Activity_1, target = NexusOperation_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_regression_nexus_start_activity_Activity_1_NexusOperation_1() {
    print "UMPIRE_ACTION regression.nexus.start_activity activity=Activity#1 operation=NexusOperation#1";
    exists_Activity += (Activity_1);
    state_Activity[Activity_1] = Activity_state_completed;
    state_NexusOperation[NexusOperation_1] = NexusOperation_state_succeeded;
    relation_nexus_activity += ((source = NexusOperation_1, target = Activity_1));
    relation_activity_nexus += ((source = Activity_1, target = NexusOperation_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun CheckSafety() {
    CheckRelation_0();
    CheckRelation_1();
    CheckProperty_0();
    CheckProperty_1();
    CheckProperty_2();
  }

  fun CheckRelation_0() {
    assert !((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus) || (Activity_0 in exists_Activity && NexusOperation_0 in exists_NexusOperation), "relation activity-nexus has an absent endpoint";
    assert !((source = Activity_0, target = NexusOperation_1) in relation_activity_nexus) || (Activity_0 in exists_Activity && NexusOperation_1 in exists_NexusOperation), "relation activity-nexus has an absent endpoint";
    assert !((source = Activity_1, target = NexusOperation_0) in relation_activity_nexus) || (Activity_1 in exists_Activity && NexusOperation_0 in exists_NexusOperation), "relation activity-nexus has an absent endpoint";
    assert !((source = Activity_1, target = NexusOperation_1) in relation_activity_nexus) || (Activity_1 in exists_Activity && NexusOperation_1 in exists_NexusOperation), "relation activity-nexus has an absent endpoint";
    assert !((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus && (source = Activity_0, target = NexusOperation_1) in relation_activity_nexus), "relation activity-nexus exceeds source cardinality";
    assert !((source = Activity_1, target = NexusOperation_0) in relation_activity_nexus && (source = Activity_1, target = NexusOperation_1) in relation_activity_nexus), "relation activity-nexus exceeds source cardinality";
    assert !((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus && (source = Activity_1, target = NexusOperation_0) in relation_activity_nexus), "relation activity-nexus exceeds target cardinality";
    assert !((source = Activity_0, target = NexusOperation_1) in relation_activity_nexus && (source = Activity_1, target = NexusOperation_1) in relation_activity_nexus), "relation activity-nexus exceeds target cardinality";
  }

  fun CheckRelation_1() {
    assert !((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity) || (NexusOperation_0 in exists_NexusOperation && Activity_0 in exists_Activity), "relation nexus-activity has an absent endpoint";
    assert !((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity) || (NexusOperation_0 in exists_NexusOperation && Activity_1 in exists_Activity), "relation nexus-activity has an absent endpoint";
    assert !((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity) || (NexusOperation_1 in exists_NexusOperation && Activity_0 in exists_Activity), "relation nexus-activity has an absent endpoint";
    assert !((source = NexusOperation_1, target = Activity_1) in relation_nexus_activity) || (NexusOperation_1 in exists_NexusOperation && Activity_1 in exists_Activity), "relation nexus-activity has an absent endpoint";
    assert !((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity && (source = NexusOperation_0, target = Activity_1) in relation_nexus_activity), "relation nexus-activity exceeds source cardinality";
    assert !((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity && (source = NexusOperation_1, target = Activity_1) in relation_nexus_activity), "relation nexus-activity exceeds source cardinality";
    assert !((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity && (source = NexusOperation_1, target = Activity_0) in relation_nexus_activity), "relation nexus-activity exceeds target cardinality";
    assert !((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity && (source = NexusOperation_1, target = Activity_1) in relation_nexus_activity), "relation nexus-activity exceeds target cardinality";
  }

  fun CheckProperty_0() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity) || ((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus)))) && (!(Activity_1 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity) || ((source = Activity_1, target = NexusOperation_0) in relation_activity_nexus))))))) && (!(NexusOperation_1 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity) || ((source = Activity_0, target = NexusOperation_1) in relation_activity_nexus)))) && (!(Activity_1 in exists_Activity) || ((!((source = NexusOperation_1, target = Activity_1) in relation_nexus_activity) || ((source = Activity_1, target = NexusOperation_1) in relation_activity_nexus)))))))), "property NexusActivityForwardLinkConsistency failed";
  }

  fun CheckProperty_1() {
    assert ((!(Activity_0 in exists_Activity) || (((!(NexusOperation_0 in exists_NexusOperation) || ((!((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus) || ((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity)))) && (!(NexusOperation_1 in exists_NexusOperation) || ((!((source = Activity_0, target = NexusOperation_1) in relation_activity_nexus) || ((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity))))))) && (!(Activity_1 in exists_Activity) || (((!(NexusOperation_0 in exists_NexusOperation) || ((!((source = Activity_1, target = NexusOperation_0) in relation_activity_nexus) || ((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity)))) && (!(NexusOperation_1 in exists_NexusOperation) || ((!((source = Activity_1, target = NexusOperation_1) in relation_activity_nexus) || ((source = NexusOperation_1, target = Activity_1) in relation_nexus_activity)))))))), "property NexusActivityReverseLinkConsistency failed";
  }

  fun CheckProperty_2() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity) || ((state_NexusOperation[NexusOperation_0] == NexusOperation_state_succeeded && state_Activity[Activity_0] == Activity_state_completed))))) && (!(Activity_1 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity) || ((state_NexusOperation[NexusOperation_0] == NexusOperation_state_succeeded && state_Activity[Activity_1] == Activity_state_completed)))))))) && (!(NexusOperation_1 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity) || ((state_NexusOperation[NexusOperation_1] == NexusOperation_state_succeeded && state_Activity[Activity_0] == Activity_state_completed))))) && (!(Activity_1 in exists_Activity) || ((!((source = NexusOperation_1, target = Activity_1) in relation_nexus_activity) || ((state_NexusOperation[NexusOperation_1] == NexusOperation_state_succeeded && state_Activity[Activity_1] == Activity_state_completed))))))))), "property NexusActivityTerminalRefinement failed";
  }

  fun CheckQuiescent() {
    CheckQuiescentProperty_0();
    CheckQuiescentProperty_1();
    CheckQuiescentProperty_2();
    CheckQuiescentProperty_3();
    CheckQuiescentProperty_4();
    CheckQuiescentProperty_5();
  }

  fun CheckQuiescentProperty_0() {
    assert ((!(Activity_0 in exists_Activity) || (!(state_Activity[Activity_0] == Activity_state_backing_off))) && (!(Activity_1 in exists_Activity) || (!(state_Activity[Activity_1] == Activity_state_backing_off)))), "quiescent property Activity.backing_off.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_1() {
    assert ((!(Activity_0 in exists_Activity) || (!(state_Activity[Activity_0] == Activity_state_scheduled))) && (!(Activity_1 in exists_Activity) || (!(state_Activity[Activity_1] == Activity_state_scheduled)))), "quiescent property Activity.scheduled.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_2() {
    assert ((!(Activity_0 in exists_Activity) || (!(state_Activity[Activity_0] == Activity_state_started))) && (!(Activity_1 in exists_Activity) || (!(state_Activity[Activity_1] == Activity_state_started)))), "quiescent property Activity.started.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_3() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off))) && (!(NexusOperation_1 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off)))), "quiescent property NexusOperation.backing_off.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_4() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled))) && (!(NexusOperation_1 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled)))), "quiescent property NexusOperation.scheduled.quiescent-progress failed";
  }

  fun CheckQuiescentProperty_5() {
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_0] == NexusOperation_state_started))) && (!(NexusOperation_1 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_1] == NexusOperation_state_started)))), "quiescent property NexusOperation.started.quiescent-progress failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
