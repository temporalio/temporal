// Generated from the Umpire verification snapshot. Do not edit.

event eStep;

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
      if (sizeof(enabled) == 1) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_backing_off_timeout_AnyHosting_Activity_0(); return; }
      enabled -= (1);
    }
    if (2 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_scheduled_cancel_AnyHosting_Activity_0(); return; }
      enabled -= (2);
    }
    if (3 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_fail_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_scheduled_fail_AnyHosting_Activity_0(); return; }
      enabled -= (3);
    }
    if (4 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_scheduled_timeout_AnyHosting_Activity_0(); return; }
      enabled -= (4);
    }
    if (5 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_attempt_failed_AnyHosting_Activity_0(); return; }
      enabled -= (5);
    }
    if (6 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_cancel_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_cancel_AnyHosting_Activity_0(); return; }
      enabled -= (6);
    }
    if (7 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_complete_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_complete_AnyHosting_Activity_0(); return; }
      enabled -= (7);
    }
    if (8 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_fail_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_fail_AnyHosting_Activity_0(); return; }
      enabled -= (8);
    }
    if (9 in enabled) {
      if (sizeof(enabled) == 1) { Apply_Activity_started_timeout_AnyHosting_Activity_0(); return; }
      if ($) { Apply_Activity_started_timeout_AnyHosting_Activity_0(); return; }
      enabled -= (9);
    }
    if (10 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (10);
    }
    if (11 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (11);
    }
    if (12 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (12);
    }
    if (13 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (13);
    }
    if (14 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (14);
    }
    if (15 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (15);
    }
    if (16 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (16);
    }
    if (17 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_activity_delivery_authorize_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (17);
    }
    if (18 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (18);
    }
    if (19 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (19);
    }
    if (20 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (20);
    }
    if (21 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_persist_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (21);
    }
    if (22 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (22);
    }
    if (23 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (23);
    }
    if (24 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (24);
    }
    if (25 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_persist_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (25);
    }
    if (26 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (26);
    }
    if (27 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (27);
    }
    if (28 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (28);
    }
    if (29 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_activity_delivery_reject_Activity_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (29);
    }
    if (30 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (30);
    }
    if (31 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (31);
    }
    if (32 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (32);
    }
    if (33 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_activity_delivery_reject_Activity_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (33);
    }
    if (34 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_activity_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (34);
    }
    if (35 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_activity_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (35);
    }
    if (36 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_activity_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (36);
    }
    if (37 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_activity_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (37);
    }
    if (38 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (38);
    }
    if (39 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (39);
    }
    if (40 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (40);
    }
    if (41 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (41);
    }
    if (42 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (42);
    }
    if (43 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (43);
    }
    if (44 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (44);
    }
    if (45 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (45);
    }
    if (46 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (46);
    }
    if (47 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (47);
    }
    if (48 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (48);
    }
    if (49 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (49);
    }
    if (50 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (50);
    }
    if (51 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (51);
    }
    if (52 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (52);
    }
    if (53 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_0_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (53);
    }
    if (54 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (54);
    }
    if (55 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (55);
    }
    if (56 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (56);
    }
    if (57 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (57);
    }
    if (58 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (58);
    }
    if (59 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (59);
    }
    if (60 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (60);
    }
    if (61 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (61);
    }
    if (62 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (62);
    }
    if (63 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (63);
    }
    if (64 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (64);
    }
    if (65 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (65);
    }
    if (66 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (66);
    }
    if (67 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (67);
    }
    if (68 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (68);
    }
    if (69 in enabled) {
      if (sizeof(enabled) == 1) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_activity_delivery_retry_Activity_0_WorkObligation_1_DeliveryTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (69);
    }
    if (70 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (70);
    }
    if (71 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (71);
    }
    if (72 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (72);
    }
    if (73 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (73);
    }
    if (74 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (74);
    }
    if (75 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (75);
    }
    if (76 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (76);
    }
    if (77 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (77);
    }
    if (78 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (78);
    }
    if (79 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (79);
    }
    if (80 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (80);
    }
    if (81 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (81);
    }
    if (82 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      enabled -= (82);
    }
    if (83 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
      enabled -= (83);
    }
    if (84 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (84);
    }
    if (85 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (85);
    }
    if (86 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (86);
    }
    if (87 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (87);
    }
    if (88 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (88);
    }
    if (89 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (89);
    }
    if (90 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (90);
    }
    if (91 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (91);
    }
    if (92 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_0(); return; }
      enabled -= (92);
    }
    if (93 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_1(); return; }
      enabled -= (93);
    }
    if (94 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_0(); return; }
      enabled -= (94);
    }
    if (95 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_1(); return; }
      enabled -= (95);
    }
    if (96 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_0(); return; }
      enabled -= (96);
    }
    if (97 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_1(); return; }
      enabled -= (97);
    }
    if (98 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_0(); return; }
      enabled -= (98);
    }
    if (99 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_1(); return; }
      enabled -= (99);
    }
    if (100 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retire_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_retire_DeliveryTask_0(); return; }
      enabled -= (100);
    }
    if (101 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retire_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_retire_DeliveryTask_1(); return; }
      enabled -= (101);
    }
    if (102 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (102);
    }
    if (103 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (103);
    }
    if (104 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (104);
    }
    if (105 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (105);
    }
    if (106 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_spool_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_spool_DeliveryTask_0(); return; }
      enabled -= (106);
    }
    if (107 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_spool_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_spool_DeliveryTask_1(); return; }
      enabled -= (107);
    }
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
    assert !((source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task) || (Activity_0 in exists_Activity && DeliveryTask_0 in exists_DeliveryTask), "relation activity-delivery-task has an absent endpoint";
    assert !((source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task) || (Activity_0 in exists_Activity && DeliveryTask_1 in exists_DeliveryTask), "relation activity-delivery-task has an absent endpoint";
    assert !((source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task && (source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task), "relation activity-delivery-task exceeds source cardinality";
    assert !((source = Activity_0, target = WorkObligation_0) in relation_activity_obligation) || (Activity_0 in exists_Activity && WorkObligation_0 in exists_WorkObligation), "relation activity-obligation has an absent endpoint";
    assert !((source = Activity_0, target = WorkObligation_1) in relation_activity_obligation) || (Activity_0 in exists_Activity && WorkObligation_1 in exists_WorkObligation), "relation activity-obligation has an absent endpoint";
    assert !((source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && (source = Activity_0, target = WorkObligation_1) in relation_activity_obligation), "relation activity-obligation exceeds source cardinality";
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
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_0 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_0 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (DeliveryTask_1 in exists_DeliveryTask && WorkObligation_1 in exists_WorkObligation), "relation delivery-task-obligation has an absent endpoint";
    assert !((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation), "relation delivery-task-obligation exceeds source cardinality";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_0 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_0 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue) || (DeliveryTask_1 in exists_DeliveryTask && DeliveryQueue_1 in exists_DeliveryQueue), "relation delivery-task-queue has an absent endpoint";
    assert !((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
    assert !((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue && (source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue), "relation delivery-task-queue exceeds source cardinality";
    assert ((!(Activity_0 in exists_Activity) || ((!(state_Activity[Activity_0] == Activity_state_started) || (((WorkObligation_0 in exists_WorkObligation && (((source = Activity_0, target = WorkObligation_0) in relation_activity_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))) || (WorkObligation_1 in exists_WorkObligation && (((source = Activity_0, target = WorkObligation_1) in relation_activity_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))))))))), "property activity.delivery.accepted-start-correspondence failed";
    assert ((!(Activity_0 in exists_Activity) || ((!((state_Activity[Activity_0] == Activity_state_scheduled || state_Activity[Activity_0] == Activity_state_started || state_Activity[Activity_0] == Activity_state_backing_off)) || ((((WorkObligation_0 in exists_WorkObligation && ((source = Activity_0, target = WorkObligation_0) in relation_activity_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = Activity_0, target = WorkObligation_1) in relation_activity_obligation))) && ((DeliveryTask_0 in exists_DeliveryTask && ((source = Activity_0, target = DeliveryTask_0) in relation_activity_delivery_task)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = Activity_0, target = DeliveryTask_1) in relation_activity_delivery_task))))))))), "property activity.delivery.intent-correspondence failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal)))))))))), "property delivery.coarse-retirement-safety failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue)))))), "property delivery.destination-isolation failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))))), "property delivery.failed-start-is-not-accepted failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))))))))), "property delivery.no-phantom-dispatch failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired))))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))))), "property delivery.no-resurrection failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))), "property delivery.no-split-commit failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue)))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue))))))), "property delivery.path-equivalence failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))))), "property delivery.retry-preserves-obligation failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))))), "property delivery.single-accepted-start failed";
  }

  fun CheckQuiescent() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved))) && (!(WorkObligation_1 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved)))), "quiescent property delivery.ambiguous-commit-resolved failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
