// Generated from the Umpire verification snapshot. Do not edit.

event eStep;

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
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (176); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (177); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (178); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (179); }
    if (WorkflowTask_0 in exists_WorkflowTask && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid))))))) { enabled += (180); }
    if (WorkflowTask_0 in exists_WorkflowTask && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid))))))) { enabled += (181); }
    if (WorkflowTask_1 in exists_WorkflowTask && DeliveryTask_0 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid))))))) { enabled += (182); }
    if (WorkflowTask_1 in exists_WorkflowTask && DeliveryTask_1 in exists_DeliveryTask && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid))))))) { enabled += (183); }
    if (sizeof(enabled) == 0) {
      CheckQuiescent();
      raise halt;
    }
    if (0 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowRun_started_cancel_AnyHosting_WorkflowRun_0(); return; }
      if ($) { Apply_WorkflowRun_started_cancel_AnyHosting_WorkflowRun_0(); return; }
      enabled -= (0);
    }
    if (1 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowRun_started_complete_AnyHosting_WorkflowRun_0(); return; }
      if ($) { Apply_WorkflowRun_started_complete_AnyHosting_WorkflowRun_0(); return; }
      enabled -= (1);
    }
    if (2 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowRun_started_continue_as_new_AnyHosting_WorkflowRun_0(); return; }
      if ($) { Apply_WorkflowRun_started_continue_as_new_AnyHosting_WorkflowRun_0(); return; }
      enabled -= (2);
    }
    if (3 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowRun_started_fail_AnyHosting_WorkflowRun_0(); return; }
      if ($) { Apply_WorkflowRun_started_fail_AnyHosting_WorkflowRun_0(); return; }
      enabled -= (3);
    }
    if (4 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowRun_started_terminate_AnyHosting_WorkflowRun_0(); return; }
      if ($) { Apply_WorkflowRun_started_terminate_AnyHosting_WorkflowRun_0(); return; }
      enabled -= (4);
    }
    if (5 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowRun_started_timeout_AnyHosting_WorkflowRun_0(); return; }
      if ($) { Apply_WorkflowRun_started_timeout_AnyHosting_WorkflowRun_0(); return; }
      enabled -= (5);
    }
    if (6 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_added_poll_AnyHosting_WorkflowTask_0(); return; }
      if ($) { Apply_WorkflowTask_added_poll_AnyHosting_WorkflowTask_0(); return; }
      enabled -= (6);
    }
    if (7 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_added_poll_AnyHosting_WorkflowTask_1(); return; }
      if ($) { Apply_WorkflowTask_added_poll_AnyHosting_WorkflowTask_1(); return; }
      enabled -= (7);
    }
    if (8 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_added_terminate_AnyHosting_WorkflowTask_0(); return; }
      if ($) { Apply_WorkflowTask_added_terminate_AnyHosting_WorkflowTask_0(); return; }
      enabled -= (8);
    }
    if (9 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_added_terminate_AnyHosting_WorkflowTask_1(); return; }
      if ($) { Apply_WorkflowTask_added_terminate_AnyHosting_WorkflowTask_1(); return; }
      enabled -= (9);
    }
    if (10 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_created_terminate_AnyHosting_WorkflowTask_0(); return; }
      if ($) { Apply_WorkflowTask_created_terminate_AnyHosting_WorkflowTask_0(); return; }
      enabled -= (10);
    }
    if (11 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_created_terminate_AnyHosting_WorkflowTask_1(); return; }
      if ($) { Apply_WorkflowTask_created_terminate_AnyHosting_WorkflowTask_1(); return; }
      enabled -= (11);
    }
    if (12 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_stored_poll_AnyHosting_WorkflowTask_0(); return; }
      if ($) { Apply_WorkflowTask_stored_poll_AnyHosting_WorkflowTask_0(); return; }
      enabled -= (12);
    }
    if (13 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_stored_poll_AnyHosting_WorkflowTask_1(); return; }
      if ($) { Apply_WorkflowTask_stored_poll_AnyHosting_WorkflowTask_1(); return; }
      enabled -= (13);
    }
    if (14 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_0(); return; }
      if ($) { Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_0(); return; }
      enabled -= (14);
    }
    if (15 in enabled) {
      if (sizeof(enabled) == 1) { Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_1(); return; }
      if ($) { Apply_WorkflowTask_stored_terminate_AnyHosting_WorkflowTask_1(); return; }
      enabled -= (15);
    }
    if (16 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (16);
    }
    if (17 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (17);
    }
    if (18 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (18);
    }
    if (19 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (19);
    }
    if (20 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (20);
    }
    if (21 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (21);
    }
    if (22 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (22);
    }
    if (23 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (23);
    }
    if (24 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (24);
    }
    if (25 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (25);
    }
    if (26 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (26);
    }
    if (27 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (27);
    }
    if (28 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (28);
    }
    if (29 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (29);
    }
    if (30 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (30);
    }
    if (31 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (31);
    }
    if (32 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (32);
    }
    if (33 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (33);
    }
    if (34 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (34);
    }
    if (35 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (35);
    }
    if (36 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (36);
    }
    if (37 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (37);
    }
    if (38 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (38);
    }
    if (39 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (39);
    }
    if (40 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      enabled -= (40);
    }
    if (41 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
      enabled -= (41);
    }
    if (42 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (42);
    }
    if (43 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (43);
    }
    if (44 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (44);
    }
    if (45 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (45);
    }
    if (46 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (46);
    }
    if (47 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (47);
    }
    if (48 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (48);
    }
    if (49 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (49);
    }
    if (50 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_0(); return; }
      enabled -= (50);
    }
    if (51 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_1(); return; }
      enabled -= (51);
    }
    if (52 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_0(); return; }
      enabled -= (52);
    }
    if (53 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_1(); return; }
      enabled -= (53);
    }
    if (54 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_0(); return; }
      enabled -= (54);
    }
    if (55 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_1(); return; }
      enabled -= (55);
    }
    if (56 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_0(); return; }
      enabled -= (56);
    }
    if (57 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_1(); return; }
      enabled -= (57);
    }
    if (58 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retire_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_retire_DeliveryTask_0(); return; }
      enabled -= (58);
    }
    if (59 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retire_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_retire_DeliveryTask_1(); return; }
      enabled -= (59);
    }
    if (60 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (60);
    }
    if (61 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (61);
    }
    if (62 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (62);
    }
    if (63 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (63);
    }
    if (64 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (64);
    }
    if (65 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (65);
    }
    if (66 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (66);
    }
    if (67 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (67);
    }
    if (68 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (68);
    }
    if (69 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (69);
    }
    if (70 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (70);
    }
    if (71 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (71);
    }
    if (72 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (72);
    }
    if (73 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (73);
    }
    if (74 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (74);
    }
    if (75 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (75);
    }
    if (76 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (76);
    }
    if (77 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (77);
    }
    if (78 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (78);
    }
    if (79 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (79);
    }
    if (80 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (80);
    }
    if (81 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (81);
    }
    if (82 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (82);
    }
    if (83 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (83);
    }
    if (84 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (84);
    }
    if (85 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (85);
    }
    if (86 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (86);
    }
    if (87 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (87);
    }
    if (88 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (88);
    }
    if (89 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (89);
    }
    if (90 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (90);
    }
    if (91 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (91);
    }
    if (92 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (92);
    }
    if (93 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (93);
    }
    if (94 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (94);
    }
    if (95 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_authorize_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (95);
    }
    if (96 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (96);
    }
    if (97 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (97);
    }
    if (98 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (98);
    }
    if (99 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (99);
    }
    if (100 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (100);
    }
    if (101 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (101);
    }
    if (102 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (102);
    }
    if (103 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (103);
    }
    if (104 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (104);
    }
    if (105 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (105);
    }
    if (106 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (106);
    }
    if (107 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (107);
    }
    if (108 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (108);
    }
    if (109 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (109);
    }
    if (110 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (110);
    }
    if (111 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (111);
    }
    if (112 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (112);
    }
    if (113 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (113);
    }
    if (114 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (114);
    }
    if (115 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (115);
    }
    if (116 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (116);
    }
    if (117 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (117);
    }
    if (118 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (118);
    }
    if (119 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (119);
    }
    if (120 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (120);
    }
    if (121 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (121);
    }
    if (122 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (122);
    }
    if (123 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (123);
    }
    if (124 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (124);
    }
    if (125 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (125);
    }
    if (126 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (126);
    }
    if (127 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_workflow_delivery_reject_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (127);
    }
    if (128 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (128);
    }
    if (129 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (129);
    }
    if (130 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (130);
    }
    if (131 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_0_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (131);
    }
    if (132 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (132);
    }
    if (133 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (133);
    }
    if (134 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (134);
    }
    if (135 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_added_WorkflowTask_1_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (135);
    }
    if (136 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (136);
    }
    if (137 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (137);
    }
    if (138 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (138);
    }
    if (139 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_0_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (139);
    }
    if (140 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (140);
    }
    if (141 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (141);
    }
    if (142 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (142);
    }
    if (143 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_cancel_speculative_stored_WorkflowTask_1_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (143);
    }
    if (144 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (144);
    }
    if (145 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (145);
    }
    if (146 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (146);
    }
    if (147 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (147);
    }
    if (148 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (148);
    }
    if (149 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (149);
    }
    if (150 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (150);
    }
    if (151 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (151);
    }
    if (152 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (152);
    }
    if (153 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (153);
    }
    if (154 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (154);
    }
    if (155 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (155);
    }
    if (156 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (156);
    }
    if (157 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (157);
    }
    if (158 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (158);
    }
    if (159 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_normal_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (159);
    }
    if (160 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (160);
    }
    if (161 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (161);
    }
    if (162 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (162);
    }
    if (163 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (163);
    }
    if (164 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (164);
    }
    if (165 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (165);
    }
    if (166 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (166);
    }
    if (167 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_0_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (167);
    }
    if (168 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (168);
    }
    if (169 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (169);
    }
    if (170 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (170);
    }
    if (171 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (171);
    }
    if (172 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (172);
    }
    if (173 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (173);
    }
    if (174 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (174);
    }
    if (175 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_workflow_task_create_speculative_direct_WorkflowRun_0_WorkflowTask_1_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (175);
    }
    if (176 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_resolve_normal_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_resolve_normal_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (176);
    }
    if (177 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_resolve_normal_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_resolve_normal_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (177);
    }
    if (178 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_resolve_normal_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_resolve_normal_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (178);
    }
    if (179 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_resolve_normal_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_resolve_normal_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (179);
    }
    if (180 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_speculative_fallback_WorkflowTask_0_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_speculative_fallback_WorkflowTask_0_DeliveryTask_0(); return; }
      enabled -= (180);
    }
    if (181 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_speculative_fallback_WorkflowTask_0_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_speculative_fallback_WorkflowTask_0_DeliveryTask_1(); return; }
      enabled -= (181);
    }
    if (182 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_speculative_fallback_WorkflowTask_1_DeliveryTask_0(); return; }
      if ($) { Apply_workflow_task_speculative_fallback_WorkflowTask_1_DeliveryTask_0(); return; }
      enabled -= (182);
    }
    if (183 in enabled) {
      if (sizeof(enabled) == 1) { Apply_workflow_task_speculative_fallback_WorkflowTask_1_DeliveryTask_1(); return; }
      if ($) { Apply_workflow_task_speculative_fallback_WorkflowTask_1_DeliveryTask_1(); return; }
      enabled -= (183);
    }
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
    assert !((source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task) || (WorkflowTask_0 in exists_WorkflowTask && DeliveryTask_0 in exists_DeliveryTask), "relation workflow-task-delivery-task has an absent endpoint";
    assert !((source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task) || (WorkflowTask_0 in exists_WorkflowTask && DeliveryTask_1 in exists_DeliveryTask), "relation workflow-task-delivery-task has an absent endpoint";
    assert !((source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task) || (WorkflowTask_1 in exists_WorkflowTask && DeliveryTask_0 in exists_DeliveryTask), "relation workflow-task-delivery-task has an absent endpoint";
    assert !((source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task) || (WorkflowTask_1 in exists_WorkflowTask && DeliveryTask_1 in exists_DeliveryTask), "relation workflow-task-delivery-task has an absent endpoint";
    assert !((source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task), "relation workflow-task-delivery-task exceeds source cardinality";
    assert !((source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task), "relation workflow-task-delivery-task exceeds source cardinality";
    assert !((source = WorkflowTask_0, target = DeliveryTask_0) in relation_workflow_task_delivery_task && (source = WorkflowTask_1, target = DeliveryTask_0) in relation_workflow_task_delivery_task), "relation workflow-task-delivery-task exceeds target cardinality";
    assert !((source = WorkflowTask_0, target = DeliveryTask_1) in relation_workflow_task_delivery_task && (source = WorkflowTask_1, target = DeliveryTask_1) in relation_workflow_task_delivery_task), "relation workflow-task-delivery-task exceeds target cardinality";
    assert !((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run) || (WorkflowTask_0 in exists_WorkflowTask && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-task-normal-run has an absent endpoint";
    assert !((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run) || (WorkflowTask_1 in exists_WorkflowTask && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-task-normal-run has an absent endpoint";
    assert !((source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation) || (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation), "relation workflow-task-obligation has an absent endpoint";
    assert !((source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation) || (WorkflowTask_0 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation), "relation workflow-task-obligation has an absent endpoint";
    assert !((source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation) || (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_0 in exists_WorkObligation), "relation workflow-task-obligation has an absent endpoint";
    assert !((source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation) || (WorkflowTask_1 in exists_WorkflowTask && WorkObligation_1 in exists_WorkObligation), "relation workflow-task-obligation has an absent endpoint";
    assert !((source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation), "relation workflow-task-obligation exceeds source cardinality";
    assert !((source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation), "relation workflow-task-obligation exceeds source cardinality";
    assert !((source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation), "relation workflow-task-obligation exceeds target cardinality";
    assert !((source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && (source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation), "relation workflow-task-obligation exceeds target cardinality";
    assert !((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_speculative_run) || (WorkflowTask_0 in exists_WorkflowTask && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-task-speculative-run has an absent endpoint";
    assert !((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_speculative_run) || (WorkflowTask_1 in exists_WorkflowTask && WorkflowRun_0 in exists_WorkflowRun), "relation workflow-task-speculative-run has an absent endpoint";
    assert ((!(WorkflowRun_0 in exists_WorkflowRun) || (!(((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored) && ((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_speculative_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_speculative_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored)))))))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_normal_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored) && ((WorkflowTask_0 in exists_WorkflowTask && (((source = WorkflowTask_0, target = WorkflowRun_0) in relation_workflow_task_speculative_run && (state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_stored)))) || (WorkflowTask_1 in exists_WorkflowTask && (((source = WorkflowTask_1, target = WorkflowRun_0) in relation_workflow_task_speculative_run && (state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_added || state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_stored))))))))))))), "property SpeculativeTaskCreation failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal))))))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!(state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired) || (((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal)))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted || state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal)))))))))), "property delivery.coarse-retirement-safety failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || (((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue)))))), "property delivery.destination-isolation failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!(state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_rejected) || (((!(WorkObligation_0 in exists_WorkObligation) || (!((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))) && (!(WorkObligation_1 in exists_WorkObligation) || (!((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))))), "property delivery.failed-start-is-not-accepted failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)))))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || ((!((state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_failed || state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_completed)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted))))))))))))), "property delivery.no-phantom-dispatch failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired))))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_terminal) || (((!(DeliveryTask_0 in exists_DeliveryTask) || ((!((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((!((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation) || (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))))), "property delivery.no-resurrection failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!((state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid || state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted)) || (((DeliveryTask_0 in exists_DeliveryTask && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) || (DeliveryTask_1 in exists_DeliveryTask && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))), "property delivery.no-split-commit failed";
    assert ((!(DeliveryTask_0 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_0, target = DeliveryQueue_1) in relation_delivery_task_queue)))))) && (!(DeliveryTask_1 in exists_DeliveryTask) || ((((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))) && ((DeliveryQueue_0 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_0) in relation_delivery_task_queue)) || (DeliveryQueue_1 in exists_DeliveryQueue && ((source = DeliveryTask_1, target = DeliveryQueue_1) in relation_delivery_task_queue))))))), "property delivery.path-equivalence failed";
    assert ((!(DeliveryAttempt_0 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation))))))))) && (!(DeliveryAttempt_1 in exists_DeliveryAttempt) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)))))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task && ((WorkObligation_0 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) || (WorkObligation_1 in exists_WorkObligation && ((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)))))))))), "property delivery.retry-preserves-obligation failed";
    assert ((!(WorkObligation_0 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))) && (!(WorkObligation_1 in exists_WorkObligation) || ((!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted) || (((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))))), "property delivery.single-accepted-start failed";
    assert ((!(WorkflowTask_0 in exists_WorkflowTask) || ((!(state_WorkflowTask[WorkflowTask_0] == WorkflowTask_state_polled) || (((WorkObligation_0 in exists_WorkObligation && (((source = WorkflowTask_0, target = WorkObligation_0) in relation_workflow_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))) || (WorkObligation_1 in exists_WorkObligation && (((source = WorkflowTask_0, target = WorkObligation_1) in relation_workflow_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start))))))))))) && (!(WorkflowTask_1 in exists_WorkflowTask) || ((!(state_WorkflowTask[WorkflowTask_1] == WorkflowTask_state_polled) || (((WorkObligation_0 in exists_WorkObligation && (((source = WorkflowTask_1, target = WorkObligation_0) in relation_workflow_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_0, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))) || (WorkObligation_1 in exists_WorkObligation && (((source = WorkflowTask_1, target = WorkObligation_1) in relation_workflow_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_accepted && ((DeliveryAttempt_0 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_0) in relation_delivery_accepted_start)) || (DeliveryAttempt_1 in exists_DeliveryAttempt && ((source = WorkObligation_1, target = DeliveryAttempt_1) in relation_delivery_accepted_start)))))))))))), "property workflow.delivery.accepted-start-correspondence failed";
  }

  fun CheckQuiescent() {
    assert ((!(WorkObligation_0 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved))) && (!(WorkObligation_1 in exists_WorkObligation) || (!(state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved)))), "quiescent property delivery.ambiguous-commit-resolved failed";
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
