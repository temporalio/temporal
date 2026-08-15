// Generated from the Umpire verification snapshot. Do not edit.

event eStep;

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
    if (!(Activity_1 in exists_Activity) && NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (96); }
    if (!(Activity_1 in exists_Activity) && NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (97); }
    if (sizeof(enabled) == 0) {
      CheckQuiescent();
      raise halt;
    }
    if (0 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_backing_off_cancel_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_backing_off_cancel_AnyHosting_Activity_0(); return; }
      enabled -= (0);
    }
    if (1 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_backing_off_cancel_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_backing_off_cancel_AnyHosting_Activity_1(); return; }
      enabled -= (1);
    }
    if (2 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_backing_off_schedule_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_backing_off_schedule_AnyHosting_Activity_0(); return; }
      enabled -= (2);
    }
    if (3 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_backing_off_schedule_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_backing_off_schedule_AnyHosting_Activity_1(); return; }
      enabled -= (3);
    }
    if (4 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_0(); return; }
      enabled -= (4);
    }
    if (5 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_1(); return; }
      enabled -= (5);
    }
    if (6 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_0(); return; }
      enabled -= (6);
    }
    if (7 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_1(); return; }
      enabled -= (7);
    }
    if (8 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_fail_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_scheduled_fail_AnyHosting_Activity_0(); return; }
      enabled -= (8);
    }
    if (9 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_fail_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_scheduled_fail_AnyHosting_Activity_1(); return; }
      enabled -= (9);
    }
    if (10 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_start_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_scheduled_start_AnyHosting_Activity_0(); return; }
      enabled -= (10);
    }
    if (11 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_start_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_scheduled_start_AnyHosting_Activity_1(); return; }
      enabled -= (11);
    }
    if (12 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_0(); return; }
      enabled -= (12);
    }
    if (13 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_1(); return; }
      enabled -= (13);
    }
    if (14 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_0(); return; }
      enabled -= (14);
    }
    if (15 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_1(); return; }
      enabled -= (15);
    }
    if (16 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_cancel_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_cancel_AnyHosting_Activity_0(); return; }
      enabled -= (16);
    }
    if (17 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_cancel_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_started_cancel_AnyHosting_Activity_1(); return; }
      enabled -= (17);
    }
    if (18 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_complete_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_complete_AnyHosting_Activity_0(); return; }
      enabled -= (18);
    }
    if (19 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_complete_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_started_complete_AnyHosting_Activity_1(); return; }
      enabled -= (19);
    }
    if (20 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_fail_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_fail_AnyHosting_Activity_0(); return; }
      enabled -= (20);
    }
    if (21 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_fail_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_started_fail_AnyHosting_Activity_1(); return; }
      enabled -= (21);
    }
    if (22 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_timeout_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_timeout_AnyHosting_Activity_0(); return; }
      enabled -= (22);
    }
    if (23 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_timeout_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_started_timeout_AnyHosting_Activity_1(); return; }
      enabled -= (23);
    }
    if (24 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_unspecified_schedule_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_unspecified_schedule_AnyHosting_Activity_0(); return; }
      enabled -= (24);
    }
    if (25 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_unspecified_schedule_AnyHosting_Activity_1(); return; }
      if ($) { Apply_Activity_unspecified_schedule_AnyHosting_Activity_1(); return; }
      enabled -= (25);
    }
    if (26 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_0(); return; }
      enabled -= (26);
    }
    if (27 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_1(); return; }
      enabled -= (27);
    }
    if (28 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_0(); return; }
      enabled -= (28);
    }
    if (29 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_1(); return; }
      enabled -= (29);
    }
    if (30 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_0(); return; }
      enabled -= (30);
    }
    if (31 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_1(); return; }
      enabled -= (31);
    }
    if (32 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_0(); return; }
      enabled -= (32);
    }
    if (33 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_1(); return; }
      enabled -= (33);
    }
    if (34 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_0(); return; }
      enabled -= (34);
    }
    if (35 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_1(); return; }
      enabled -= (35);
    }
    if (36 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_0(); return; }
      enabled -= (36);
    }
    if (37 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_1(); return; }
      enabled -= (37);
    }
    if (38 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_0(); return; }
      enabled -= (38);
    }
    if (39 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_1(); return; }
      enabled -= (39);
    }
    if (40 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_0(); return; }
      enabled -= (40);
    }
    if (41 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_1(); return; }
      enabled -= (41);
    }
    if (42 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_0(); return; }
      enabled -= (42);
    }
    if (43 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_1(); return; }
      enabled -= (43);
    }
    if (44 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_0(); return; }
      enabled -= (44);
    }
    if (45 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_1(); return; }
      enabled -= (45);
    }
    if (46 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_0(); return; }
      enabled -= (46);
    }
    if (47 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_1(); return; }
      enabled -= (47);
    }
    if (48 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_0(); return; }
      enabled -= (48);
    }
    if (49 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_1(); return; }
      enabled -= (49);
    }
    if (50 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_0(); return; }
      enabled -= (50);
    }
    if (51 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_1(); return; }
      enabled -= (51);
    }
    if (52 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_0(); return; }
      enabled -= (52);
    }
    if (53 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_1(); return; }
      enabled -= (53);
    }
    if (54 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_0(); return; }
      enabled -= (54);
    }
    if (55 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_1(); return; }
      enabled -= (55);
    }
    if (56 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_0(); return; }
      enabled -= (56);
    }
    if (57 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_1(); return; }
      enabled -= (57);
    }
    if (58 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_0(); return; }
      enabled -= (58);
    }
    if (59 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_1(); return; }
      enabled -= (59);
    }
    if (60 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_0(); return; }
      enabled -= (60);
    }
    if (61 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_1(); return; }
      enabled -= (61);
    }
    if (62 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_0(); return; }
      enabled -= (62);
    }
    if (63 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_1(); return; }
      enabled -= (63);
    }
    if (64 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_0(); return; }
      enabled -= (64);
    }
    if (65 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_1(); return; }
      enabled -= (65);
    }
    if (66 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_0(); return; }
      enabled -= (66);
    }
    if (67 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_1(); return; }
      enabled -= (67);
    }
    if (68 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_0(); return; }
      enabled -= (68);
    }
    if (69 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_1(); return; }
      enabled -= (69);
    }
    if (70 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_0(); return; }
      enabled -= (70);
    }
    if (71 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_1(); return; }
      enabled -= (71);
    }
    if (72 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_0(); return; }
      enabled -= (72);
    }
    if (73 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_1(); return; }
      enabled -= (73);
    }
    if (74 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_0(); return; }
      enabled -= (74);
    }
    if (75 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_1(); return; }
      enabled -= (75);
    }
    if (76 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_0(); return; }
      enabled -= (76);
    }
    if (77 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_1(); return; }
      enabled -= (77);
    }
    if (78 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_0(); return; }
      enabled -= (78);
    }
    if (79 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_1(); return; }
      enabled -= (79);
    }
    if (80 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_0(); return; }
      enabled -= (80);
    }
    if (81 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_1(); return; }
      enabled -= (81);
    }
    if (82 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_0(); return; }
      enabled -= (82);
    }
    if (83 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_1(); return; }
      enabled -= (83);
    }
    if (84 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_0(); return; }
      enabled -= (84);
    }
    if (85 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_1(); return; }
      enabled -= (85);
    }
    if (86 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_0(); return; }
      enabled -= (86);
    }
    if (87 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_1(); return; }
      enabled -= (87);
    }
    if (88 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_0(); return; }
      enabled -= (88);
    }
    if (89 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_1(); return; }
      enabled -= (89);
    }
    if (90 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_0(); return; }
      enabled -= (90);
    }
    if (91 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_1(); return; }
      enabled -= (91);
    }
    if (92 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_0(); return; }
      enabled -= (92);
    }
    if (93 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_1(); return; }
      enabled -= (93);
    }
    if (94 in enabled) {
      if (sizeof(enabled) == 1) { Apply_regression_nexus_start_activity_Activity_0_NexusOperation_0(); return; }
      if ($) { Apply_regression_nexus_start_activity_Activity_0_NexusOperation_0(); return; }
      enabled -= (94);
    }
    if (95 in enabled) {
      if (sizeof(enabled) == 1) { Apply_regression_nexus_start_activity_Activity_0_NexusOperation_1(); return; }
      if ($) { Apply_regression_nexus_start_activity_Activity_0_NexusOperation_1(); return; }
      enabled -= (95);
    }
    if (96 in enabled) {
      if (sizeof(enabled) == 1) { Apply_regression_nexus_start_activity_Activity_1_NexusOperation_0(); return; }
      if ($) { Apply_regression_nexus_start_activity_Activity_1_NexusOperation_0(); return; }
      enabled -= (96);
    }
    if (97 in enabled) {
      if (sizeof(enabled) == 1) { Apply_regression_nexus_start_activity_Activity_1_NexusOperation_1(); return; }
      if ($) { Apply_regression_nexus_start_activity_Activity_1_NexusOperation_1(); return; }
      enabled -= (97);
    }
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
    assert !((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus) || (Activity_0 in exists_Activity && NexusOperation_0 in exists_NexusOperation), "relation activity-nexus has an absent endpoint";
    assert !((source = Activity_0, target = NexusOperation_1) in relation_activity_nexus) || (Activity_0 in exists_Activity && NexusOperation_1 in exists_NexusOperation), "relation activity-nexus has an absent endpoint";
    assert !((source = Activity_1, target = NexusOperation_0) in relation_activity_nexus) || (Activity_1 in exists_Activity && NexusOperation_0 in exists_NexusOperation), "relation activity-nexus has an absent endpoint";
    assert !((source = Activity_1, target = NexusOperation_1) in relation_activity_nexus) || (Activity_1 in exists_Activity && NexusOperation_1 in exists_NexusOperation), "relation activity-nexus has an absent endpoint";
    assert !((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus && (source = Activity_0, target = NexusOperation_1) in relation_activity_nexus), "relation activity-nexus exceeds source cardinality";
    assert !((source = Activity_1, target = NexusOperation_0) in relation_activity_nexus && (source = Activity_1, target = NexusOperation_1) in relation_activity_nexus), "relation activity-nexus exceeds source cardinality";
    assert !((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus && (source = Activity_1, target = NexusOperation_0) in relation_activity_nexus), "relation activity-nexus exceeds target cardinality";
    assert !((source = Activity_0, target = NexusOperation_1) in relation_activity_nexus && (source = Activity_1, target = NexusOperation_1) in relation_activity_nexus), "relation activity-nexus exceeds target cardinality";
    assert !((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity) || (NexusOperation_0 in exists_NexusOperation && Activity_0 in exists_Activity), "relation nexus-activity has an absent endpoint";
    assert !((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity) || (NexusOperation_0 in exists_NexusOperation && Activity_1 in exists_Activity), "relation nexus-activity has an absent endpoint";
    assert !((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity) || (NexusOperation_1 in exists_NexusOperation && Activity_0 in exists_Activity), "relation nexus-activity has an absent endpoint";
    assert !((source = NexusOperation_1, target = Activity_1) in relation_nexus_activity) || (NexusOperation_1 in exists_NexusOperation && Activity_1 in exists_Activity), "relation nexus-activity has an absent endpoint";
    assert !((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity && (source = NexusOperation_0, target = Activity_1) in relation_nexus_activity), "relation nexus-activity exceeds source cardinality";
    assert !((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity && (source = NexusOperation_1, target = Activity_1) in relation_nexus_activity), "relation nexus-activity exceeds source cardinality";
    assert !((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity && (source = NexusOperation_1, target = Activity_0) in relation_nexus_activity), "relation nexus-activity exceeds target cardinality";
    assert !((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity && (source = NexusOperation_1, target = Activity_1) in relation_nexus_activity), "relation nexus-activity exceeds target cardinality";
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity) || ((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus)))) && (!(Activity_1 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity) || ((source = Activity_1, target = NexusOperation_0) in relation_activity_nexus))))))) && (!(NexusOperation_1 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity) || ((source = Activity_0, target = NexusOperation_1) in relation_activity_nexus)))) && (!(Activity_1 in exists_Activity) || ((!((source = NexusOperation_1, target = Activity_1) in relation_nexus_activity) || ((source = Activity_1, target = NexusOperation_1) in relation_activity_nexus)))))))), "property NexusActivityForwardLinkConsistency failed";
    assert ((!(Activity_0 in exists_Activity) || (((!(NexusOperation_0 in exists_NexusOperation) || ((!((source = Activity_0, target = NexusOperation_0) in relation_activity_nexus) || ((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity)))) && (!(NexusOperation_1 in exists_NexusOperation) || ((!((source = Activity_0, target = NexusOperation_1) in relation_activity_nexus) || ((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity))))))) && (!(Activity_1 in exists_Activity) || (((!(NexusOperation_0 in exists_NexusOperation) || ((!((source = Activity_1, target = NexusOperation_0) in relation_activity_nexus) || ((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity)))) && (!(NexusOperation_1 in exists_NexusOperation) || ((!((source = Activity_1, target = NexusOperation_1) in relation_activity_nexus) || ((source = NexusOperation_1, target = Activity_1) in relation_nexus_activity)))))))), "property NexusActivityReverseLinkConsistency failed";
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_0) in relation_nexus_activity) || ((state_NexusOperation[NexusOperation_0] == NexusOperation_state_succeeded && state_Activity[Activity_0] == Activity_state_completed))))) && (!(Activity_1 in exists_Activity) || ((!((source = NexusOperation_0, target = Activity_1) in relation_nexus_activity) || ((state_NexusOperation[NexusOperation_0] == NexusOperation_state_succeeded && state_Activity[Activity_1] == Activity_state_completed)))))))) && (!(NexusOperation_1 in exists_NexusOperation) || (((!(Activity_0 in exists_Activity) || ((!((source = NexusOperation_1, target = Activity_0) in relation_nexus_activity) || ((state_NexusOperation[NexusOperation_1] == NexusOperation_state_succeeded && state_Activity[Activity_0] == Activity_state_completed))))) && (!(Activity_1 in exists_Activity) || ((!((source = NexusOperation_1, target = Activity_1) in relation_nexus_activity) || ((state_NexusOperation[NexusOperation_1] == NexusOperation_state_succeeded && state_Activity[Activity_1] == Activity_state_completed))))))))), "property NexusActivityTerminalRefinement failed";
  }

  fun CheckQuiescent() {
    assert ((!(Activity_0 in exists_Activity) || (!(state_Activity[Activity_0] == Activity_state_backing_off))) && (!(Activity_1 in exists_Activity) || (!(state_Activity[Activity_1] == Activity_state_backing_off)))), "quiescent property Activity.backing_off.quiescent-progress failed";
    assert ((!(Activity_0 in exists_Activity) || (!(state_Activity[Activity_0] == Activity_state_scheduled))) && (!(Activity_1 in exists_Activity) || (!(state_Activity[Activity_1] == Activity_state_scheduled)))), "quiescent property Activity.scheduled.quiescent-progress failed";
    assert ((!(Activity_0 in exists_Activity) || (!(state_Activity[Activity_0] == Activity_state_started))) && (!(Activity_1 in exists_Activity) || (!(state_Activity[Activity_1] == Activity_state_started)))), "quiescent property Activity.started.quiescent-progress failed";
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off))) && (!(NexusOperation_1 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off)))), "quiescent property NexusOperation.backing_off.quiescent-progress failed";
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled))) && (!(NexusOperation_1 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled)))), "quiescent property NexusOperation.scheduled.quiescent-progress failed";
    assert ((!(NexusOperation_0 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_0] == NexusOperation_state_started))) && (!(NexusOperation_1 in exists_NexusOperation) || (!(state_NexusOperation[NexusOperation_1] == NexusOperation_state_started)))), "quiescent property NexusOperation.started.quiescent-progress failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
