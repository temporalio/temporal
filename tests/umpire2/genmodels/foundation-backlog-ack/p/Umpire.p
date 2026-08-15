// Generated from the Umpire verification snapshot. Do not edit.

event eStep;

enum BacklogPosition { BacklogPosition_0, BacklogPosition_1 }
enum BacklogPosition_state { BacklogPosition_state_acked, BacklogPosition_state_gc, BacklogPosition_state_read, BacklogPosition_state_unread, BacklogPosition_state_unused }
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
type relation_backlog_position_precedes_tuple = (source: BacklogPosition, target: BacklogPosition);
type relation_backlog_position_task_tuple = (source: BacklogPosition, target: DeliveryTask);
type relation_delivery_accepted_start_tuple = (source: WorkObligation, target: DeliveryAttempt);
type relation_delivery_attempt_poller_tuple = (source: DeliveryAttempt, target: Poller);
type relation_delivery_attempt_task_tuple = (source: DeliveryAttempt, target: DeliveryTask);
type relation_delivery_task_obligation_tuple = (source: DeliveryTask, target: WorkObligation);
type relation_delivery_task_queue_tuple = (source: DeliveryTask, target: DeliveryQueue);

machine UmpireWorld {
  var checkerStep: int;
  var exists_BacklogPosition: set[BacklogPosition];
  var state_BacklogPosition: map[BacklogPosition, BacklogPosition_state];
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
  var relation_backlog_position_precedes: set[relation_backlog_position_precedes_tuple];
  var relation_backlog_position_task: set[relation_backlog_position_task_tuple];
  var relation_delivery_accepted_start: set[relation_delivery_accepted_start_tuple];
  var relation_delivery_attempt_poller: set[relation_delivery_attempt_poller_tuple];
  var relation_delivery_attempt_task: set[relation_delivery_attempt_task_tuple];
  var relation_delivery_task_obligation: set[relation_delivery_task_obligation_tuple];
  var relation_delivery_task_queue: set[relation_delivery_task_queue_tuple];

  start state Init {
    entry {
      state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_unused;
      state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_unused;
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
    if (BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_read && (source = BacklogPosition_0, target = DeliveryTask_0) in relation_backlog_position_task && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) && ((!(BacklogPosition_0 in exists_BacklogPosition) || ((!((source = BacklogPosition_0, target = BacklogPosition_0) in relation_backlog_position_precedes) || ((state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))))) && (!(BacklogPosition_1 in exists_BacklogPosition) || ((!((source = BacklogPosition_1, target = BacklogPosition_0) in relation_backlog_position_precedes) || ((state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc)))))))) { enabled += (0); }
    if (BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_read && (source = BacklogPosition_0, target = DeliveryTask_1) in relation_backlog_position_task && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired) && ((!(BacklogPosition_0 in exists_BacklogPosition) || ((!((source = BacklogPosition_0, target = BacklogPosition_0) in relation_backlog_position_precedes) || ((state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))))) && (!(BacklogPosition_1 in exists_BacklogPosition) || ((!((source = BacklogPosition_1, target = BacklogPosition_0) in relation_backlog_position_precedes) || ((state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc)))))))) { enabled += (1); }
    if (BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_read && (source = BacklogPosition_1, target = DeliveryTask_0) in relation_backlog_position_task && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired) && ((!(BacklogPosition_0 in exists_BacklogPosition) || ((!((source = BacklogPosition_0, target = BacklogPosition_1) in relation_backlog_position_precedes) || ((state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))))) && (!(BacklogPosition_1 in exists_BacklogPosition) || ((!((source = BacklogPosition_1, target = BacklogPosition_1) in relation_backlog_position_precedes) || ((state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc)))))))) { enabled += (2); }
    if (BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_read && (source = BacklogPosition_1, target = DeliveryTask_1) in relation_backlog_position_task && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired) && ((!(BacklogPosition_0 in exists_BacklogPosition) || ((!((source = BacklogPosition_0, target = BacklogPosition_1) in relation_backlog_position_precedes) || ((state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))))) && (!(BacklogPosition_1 in exists_BacklogPosition) || ((!((source = BacklogPosition_1, target = BacklogPosition_1) in relation_backlog_position_precedes) || ((state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc)))))))) { enabled += (3); }
    if (!(BacklogPosition_0 in exists_BacklogPosition) && BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_unread || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_read || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))) { enabled += (4); }
    if (!(BacklogPosition_0 in exists_BacklogPosition) && BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_unread || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_read || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))) { enabled += (5); }
    if (!(BacklogPosition_0 in exists_BacklogPosition) && BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_unread || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_read || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc))) { enabled += (6); }
    if (!(BacklogPosition_0 in exists_BacklogPosition) && BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_unread || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_read || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc))) { enabled += (7); }
    if (!(BacklogPosition_1 in exists_BacklogPosition) && BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_unread || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_read || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))) { enabled += (8); }
    if (!(BacklogPosition_1 in exists_BacklogPosition) && BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_unread || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_read || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))) { enabled += (9); }
    if (!(BacklogPosition_1 in exists_BacklogPosition) && BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_unread || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_read || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc))) { enabled += (10); }
    if (!(BacklogPosition_1 in exists_BacklogPosition) && BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_unread || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_read || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc))) { enabled += (11); }
    if (!(BacklogPosition_0 in exists_BacklogPosition) && DeliveryTask_0 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) { enabled += (12); }
    if (!(BacklogPosition_0 in exists_BacklogPosition) && DeliveryTask_1 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) { enabled += (13); }
    if (!(BacklogPosition_1 in exists_BacklogPosition) && DeliveryTask_0 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) { enabled += (14); }
    if (!(BacklogPosition_1 in exists_BacklogPosition) && DeliveryTask_1 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) { enabled += (15); }
    if (BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked && (source = BacklogPosition_0, target = DeliveryTask_0) in relation_backlog_position_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)) { enabled += (16); }
    if (BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked && (source = BacklogPosition_0, target = DeliveryTask_1) in relation_backlog_position_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)) { enabled += (17); }
    if (BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked && (source = BacklogPosition_1, target = DeliveryTask_0) in relation_backlog_position_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)) { enabled += (18); }
    if (BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked && (source = BacklogPosition_1, target = DeliveryTask_1) in relation_backlog_position_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)) { enabled += (19); }
    if (BacklogPosition_0 in exists_BacklogPosition && state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_unread) { enabled += (20); }
    if (BacklogPosition_1 in exists_BacklogPosition && state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_unread) { enabled += (21); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (22); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (23); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (24); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (25); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (26); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (27); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (28); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (29); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (30); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (31); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (32); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (33); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (34); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (35); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (36); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (37); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (38); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (39); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (40); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_reserved && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_reserved && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (41); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (42); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (43); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (44); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_authorized && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_accepted && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (45); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (46); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (47); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged) && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (48); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged) && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (49); }
    if (DeliveryTask_0 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (50); }
    if (DeliveryTask_1 in exists_DeliveryTask && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (51); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (52); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (53); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (54); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (55); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (56); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (57); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (58); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (59); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (60); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (61); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (62); }
    if (!(WorkObligation_0 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (63); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (64); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_0 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (65); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_0 in exists_DeliveryQueue) { enabled += (66); }
    if (!(WorkObligation_1 in exists_WorkObligation) && !(DeliveryTask_1 in exists_DeliveryTask) && DeliveryQueue_1 in exists_DeliveryQueue) { enabled += (67); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (68); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (69); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (70); }
    if (DeliveryTask_0 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_backlogged)) { enabled += (71); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (72); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_0 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (73); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_0 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (74); }
    if (DeliveryTask_1 in exists_DeliveryTask && !(DeliveryAttempt_1 in exists_DeliveryAttempt) && Poller_1 in exists_Poller && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_backlogged)) { enabled += (75); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (76); }
    if (WorkObligation_0 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_0] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation)) { enabled += (77); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_0 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending && (source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (78); }
    if (WorkObligation_1 in exists_WorkObligation && DeliveryTask_1 in exists_DeliveryTask && (state_WorkObligation[WorkObligation_1] == WorkObligation_state_unresolved && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending && (source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation)) { enabled += (79); }
    if (DeliveryTask_0 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged) { enabled += (80); }
    if (DeliveryTask_1 in exists_DeliveryTask && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged) { enabled += (81); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (82); }
    if (DeliveryTask_0 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_0) in relation_delivery_attempt_task)) { enabled += (83); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_0 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_0] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_0, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (84); }
    if (DeliveryTask_1 in exists_DeliveryTask && DeliveryAttempt_1 in exists_DeliveryAttempt && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_dispatched && state_DeliveryAttempt[DeliveryAttempt_1] == DeliveryAttempt_state_dispatched && (source = DeliveryAttempt_1, target = DeliveryTask_1) in relation_delivery_attempt_task)) { enabled += (85); }
    if (DeliveryTask_0 in exists_DeliveryTask && ((state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_0, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (86); }
    if (DeliveryTask_1 in exists_DeliveryTask && ((state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_pending || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_sync_offered) && ((WorkObligation_0 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_0) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_0] == WorkObligation_state_valid))) || (WorkObligation_1 in exists_WorkObligation && (((source = DeliveryTask_1, target = WorkObligation_1) in relation_delivery_task_obligation && state_WorkObligation[WorkObligation_1] == WorkObligation_state_valid)))))) { enabled += (87); }
    if (sizeof(enabled) == 0) {
      CheckQuiescent();
      raise halt;
    }
    if (0 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_ack_BacklogPosition_0_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_ack_BacklogPosition_0_DeliveryTask_0(); return; }
      enabled -= (0);
    }
    if (1 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_ack_BacklogPosition_0_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_ack_BacklogPosition_0_DeliveryTask_1(); return; }
      enabled -= (1);
    }
    if (2 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_ack_BacklogPosition_1_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_ack_BacklogPosition_1_DeliveryTask_0(); return; }
      enabled -= (2);
    }
    if (3 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_ack_BacklogPosition_1_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_ack_BacklogPosition_1_DeliveryTask_1(); return; }
      enabled -= (3);
    }
    if (4 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_0_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_0_DeliveryTask_0(); return; }
      enabled -= (4);
    }
    if (5 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_0_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_0_DeliveryTask_1(); return; }
      enabled -= (5);
    }
    if (6 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_1_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_1_DeliveryTask_0(); return; }
      enabled -= (6);
    }
    if (7 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_1_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_1_DeliveryTask_1(); return; }
      enabled -= (7);
    }
    if (8 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_0_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_0_DeliveryTask_0(); return; }
      enabled -= (8);
    }
    if (9 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_0_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_0_DeliveryTask_1(); return; }
      enabled -= (9);
    }
    if (10 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_1_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_1_DeliveryTask_0(); return; }
      enabled -= (10);
    }
    if (11 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_1_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_1_DeliveryTask_1(); return; }
      enabled -= (11);
    }
    if (12 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_first_BacklogPosition_0_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_append_first_BacklogPosition_0_DeliveryTask_0(); return; }
      enabled -= (12);
    }
    if (13 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_first_BacklogPosition_0_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_append_first_BacklogPosition_0_DeliveryTask_1(); return; }
      enabled -= (13);
    }
    if (14 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_first_BacklogPosition_1_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_append_first_BacklogPosition_1_DeliveryTask_0(); return; }
      enabled -= (14);
    }
    if (15 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_append_first_BacklogPosition_1_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_append_first_BacklogPosition_1_DeliveryTask_1(); return; }
      enabled -= (15);
    }
    if (16 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_gc_BacklogPosition_0_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_gc_BacklogPosition_0_DeliveryTask_0(); return; }
      enabled -= (16);
    }
    if (17 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_gc_BacklogPosition_0_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_gc_BacklogPosition_0_DeliveryTask_1(); return; }
      enabled -= (17);
    }
    if (18 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_gc_BacklogPosition_1_DeliveryTask_0(); return; }
      if ($) { Apply_backlog_gc_BacklogPosition_1_DeliveryTask_0(); return; }
      enabled -= (18);
    }
    if (19 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_gc_BacklogPosition_1_DeliveryTask_1(); return; }
      if ($) { Apply_backlog_gc_BacklogPosition_1_DeliveryTask_1(); return; }
      enabled -= (19);
    }
    if (20 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_read_BacklogPosition_0(); return; }
      if ($) { Apply_backlog_read_BacklogPosition_0(); return; }
      enabled -= (20);
    }
    if (21 in enabled) {
      if (sizeof(enabled) == 1) { Apply_backlog_read_BacklogPosition_1(); return; }
      if ($) { Apply_backlog_read_BacklogPosition_1(); return; }
      enabled -= (21);
    }
    if (22 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (22);
    }
    if (23 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (23);
    }
    if (24 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (24);
    }
    if (25 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_acknowledge_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (25);
    }
    if (26 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (26);
    }
    if (27 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (27);
    }
    if (28 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (28);
    }
    if (29 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (29);
    }
    if (30 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (30);
    }
    if (31 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (31);
    }
    if (32 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (32);
    }
    if (33 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_accept_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (33);
    }
    if (34 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (34);
    }
    if (35 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (35);
    }
    if (36 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (36);
    }
    if (37 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_0_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (37);
    }
    if (38 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (38);
    }
    if (39 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (39);
    }
    if (40 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (40);
    }
    if (41 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_authorize_reject_WorkObligation_1_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (41);
    }
    if (42 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (42);
    }
    if (43 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (43);
    }
    if (44 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (44);
    }
    if (45 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_dispatch_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (45);
    }
    if (46 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (46);
    }
    if (47 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (47);
    }
    if (48 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (48);
    }
    if (49 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_expire_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (49);
    }
    if (50 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_offer_sync_DeliveryTask_0(); return; }
      enabled -= (50);
    }
    if (51 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_offer_sync_DeliveryTask_1(); return; }
      enabled -= (51);
    }
    if (52 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (52);
    }
    if (53 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (53);
    }
    if (54 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (54);
    }
    if (55 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (55);
    }
    if (56 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (56);
    }
    if (57 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (57);
    }
    if (58 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (58);
    }
    if (59 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_ambiguous_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (59);
    }
    if (60 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (60);
    }
    if (61 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (61);
    }
    if (62 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (62);
    }
    if (63 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_0_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (63);
    }
    if (64 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_0(); return; }
      enabled -= (64);
    }
    if (65 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_0_DeliveryQueue_1(); return; }
      enabled -= (65);
    }
    if (66 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_0(); return; }
      enabled -= (66);
    }
    if (67 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      if ($) { Apply_delivery_persist_success_WorkObligation_1_DeliveryTask_1_DeliveryQueue_1(); return; }
      enabled -= (67);
    }
    if (68 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_0(); return; }
      enabled -= (68);
    }
    if (69 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_0_Poller_1(); return; }
      enabled -= (69);
    }
    if (70 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_0(); return; }
      enabled -= (70);
    }
    if (71 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_0_DeliveryAttempt_1_Poller_1(); return; }
      enabled -= (71);
    }
    if (72 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_0(); return; }
      enabled -= (72);
    }
    if (73 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_0_Poller_1(); return; }
      enabled -= (73);
    }
    if (74 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_0(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_0(); return; }
      enabled -= (74);
    }
    if (75 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_1(); return; }
      if ($) { Apply_delivery_reserve_DeliveryTask_1_DeliveryAttempt_1_Poller_1(); return; }
      enabled -= (75);
    }
    if (76 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_0(); return; }
      enabled -= (76);
    }
    if (77 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_resolve_persisted_WorkObligation_0_DeliveryTask_1(); return; }
      enabled -= (77);
    }
    if (78 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_0(); return; }
      enabled -= (78);
    }
    if (79 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_resolve_persisted_WorkObligation_1_DeliveryTask_1(); return; }
      enabled -= (79);
    }
    if (80 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retire_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_retire_DeliveryTask_0(); return; }
      enabled -= (80);
    }
    if (81 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retire_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_retire_DeliveryTask_1(); return; }
      enabled -= (81);
    }
    if (82 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_0(); return; }
      enabled -= (82);
    }
    if (83 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_0_DeliveryAttempt_1(); return; }
      enabled -= (83);
    }
    if (84 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_0(); return; }
      enabled -= (84);
    }
    if (85 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
      if ($) { Apply_delivery_retry_DeliveryTask_1_DeliveryAttempt_1(); return; }
      enabled -= (85);
    }
    if (86 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_spool_DeliveryTask_0(); return; }
      if ($) { Apply_delivery_spool_DeliveryTask_0(); return; }
      enabled -= (86);
    }
    if (87 in enabled) {
      if (sizeof(enabled) == 1) { Apply_delivery_spool_DeliveryTask_1(); return; }
      if ($) { Apply_delivery_spool_DeliveryTask_1(); return; }
      enabled -= (87);
    }
  }

  fun Apply_backlog_ack_BacklogPosition_0_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.ack position=BacklogPosition#0 task=DeliveryTask#0";
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_acked;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_ack_BacklogPosition_0_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.ack position=BacklogPosition#0 task=DeliveryTask#1";
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_acked;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_ack_BacklogPosition_1_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.ack position=BacklogPosition#1 task=DeliveryTask#0";
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_acked;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_ack_BacklogPosition_1_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.ack position=BacklogPosition#1 task=DeliveryTask#1";
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_acked;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_0_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.append-after position=BacklogPosition#0 previous=BacklogPosition#0 task=DeliveryTask#0";
    exists_BacklogPosition += (BacklogPosition_0);
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_0, target = DeliveryTask_0));
    relation_backlog_position_precedes += ((source = BacklogPosition_0, target = BacklogPosition_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_0_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.append-after position=BacklogPosition#0 previous=BacklogPosition#0 task=DeliveryTask#1";
    exists_BacklogPosition += (BacklogPosition_0);
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_0, target = DeliveryTask_1));
    relation_backlog_position_precedes += ((source = BacklogPosition_0, target = BacklogPosition_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_1_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.append-after position=BacklogPosition#0 previous=BacklogPosition#1 task=DeliveryTask#0";
    exists_BacklogPosition += (BacklogPosition_0);
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_0, target = DeliveryTask_0));
    relation_backlog_position_precedes += ((source = BacklogPosition_1, target = BacklogPosition_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_after_BacklogPosition_0_BacklogPosition_1_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.append-after position=BacklogPosition#0 previous=BacklogPosition#1 task=DeliveryTask#1";
    exists_BacklogPosition += (BacklogPosition_0);
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_0, target = DeliveryTask_1));
    relation_backlog_position_precedes += ((source = BacklogPosition_1, target = BacklogPosition_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_0_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.append-after position=BacklogPosition#1 previous=BacklogPosition#0 task=DeliveryTask#0";
    exists_BacklogPosition += (BacklogPosition_1);
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_1, target = DeliveryTask_0));
    relation_backlog_position_precedes += ((source = BacklogPosition_0, target = BacklogPosition_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_0_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.append-after position=BacklogPosition#1 previous=BacklogPosition#0 task=DeliveryTask#1";
    exists_BacklogPosition += (BacklogPosition_1);
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_1, target = DeliveryTask_1));
    relation_backlog_position_precedes += ((source = BacklogPosition_0, target = BacklogPosition_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_1_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.append-after position=BacklogPosition#1 previous=BacklogPosition#1 task=DeliveryTask#0";
    exists_BacklogPosition += (BacklogPosition_1);
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_1, target = DeliveryTask_0));
    relation_backlog_position_precedes += ((source = BacklogPosition_1, target = BacklogPosition_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_after_BacklogPosition_1_BacklogPosition_1_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.append-after position=BacklogPosition#1 previous=BacklogPosition#1 task=DeliveryTask#1";
    exists_BacklogPosition += (BacklogPosition_1);
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_1, target = DeliveryTask_1));
    relation_backlog_position_precedes += ((source = BacklogPosition_1, target = BacklogPosition_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_first_BacklogPosition_0_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.append-first position=BacklogPosition#0 task=DeliveryTask#0";
    exists_BacklogPosition += (BacklogPosition_0);
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_0, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_first_BacklogPosition_0_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.append-first position=BacklogPosition#0 task=DeliveryTask#1";
    exists_BacklogPosition += (BacklogPosition_0);
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_0, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_first_BacklogPosition_1_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.append-first position=BacklogPosition#1 task=DeliveryTask#0";
    exists_BacklogPosition += (BacklogPosition_1);
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_1, target = DeliveryTask_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_append_first_BacklogPosition_1_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.append-first position=BacklogPosition#1 task=DeliveryTask#1";
    exists_BacklogPosition += (BacklogPosition_1);
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_unread;
    relation_backlog_position_task += ((source = BacklogPosition_1, target = DeliveryTask_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_gc_BacklogPosition_0_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.gc position=BacklogPosition#0 task=DeliveryTask#0";
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_gc;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_gc_BacklogPosition_0_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.gc position=BacklogPosition#0 task=DeliveryTask#1";
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_gc;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_gc_BacklogPosition_1_DeliveryTask_0() {
    print "UMPIRE_ACTION backlog.gc position=BacklogPosition#1 task=DeliveryTask#0";
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_gc;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_gc_BacklogPosition_1_DeliveryTask_1() {
    print "UMPIRE_ACTION backlog.gc position=BacklogPosition#1 task=DeliveryTask#1";
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_gc;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_read_BacklogPosition_0() {
    print "UMPIRE_ACTION backlog.read position=BacklogPosition#0";
    state_BacklogPosition[BacklogPosition_0] = BacklogPosition_state_read;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_backlog_read_BacklogPosition_1() {
    print "UMPIRE_ACTION backlog.read position=BacklogPosition#1";
    state_BacklogPosition[BacklogPosition_1] = BacklogPosition_state_read;
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

  fun CheckSafety() {
    assert !((source = BacklogPosition_0, target = BacklogPosition_0) in relation_backlog_position_precedes) || (BacklogPosition_0 in exists_BacklogPosition && BacklogPosition_0 in exists_BacklogPosition), "relation backlog-position-precedes has an absent endpoint";
    assert !((source = BacklogPosition_0, target = BacklogPosition_1) in relation_backlog_position_precedes) || (BacklogPosition_0 in exists_BacklogPosition && BacklogPosition_1 in exists_BacklogPosition), "relation backlog-position-precedes has an absent endpoint";
    assert !((source = BacklogPosition_1, target = BacklogPosition_0) in relation_backlog_position_precedes) || (BacklogPosition_1 in exists_BacklogPosition && BacklogPosition_0 in exists_BacklogPosition), "relation backlog-position-precedes has an absent endpoint";
    assert !((source = BacklogPosition_1, target = BacklogPosition_1) in relation_backlog_position_precedes) || (BacklogPosition_1 in exists_BacklogPosition && BacklogPosition_1 in exists_BacklogPosition), "relation backlog-position-precedes has an absent endpoint";
    assert !((source = BacklogPosition_0, target = BacklogPosition_0) in relation_backlog_position_precedes && (source = BacklogPosition_0, target = BacklogPosition_1) in relation_backlog_position_precedes), "relation backlog-position-precedes exceeds source cardinality";
    assert !((source = BacklogPosition_1, target = BacklogPosition_0) in relation_backlog_position_precedes && (source = BacklogPosition_1, target = BacklogPosition_1) in relation_backlog_position_precedes), "relation backlog-position-precedes exceeds source cardinality";
    assert !((source = BacklogPosition_0, target = BacklogPosition_0) in relation_backlog_position_precedes && (source = BacklogPosition_1, target = BacklogPosition_0) in relation_backlog_position_precedes), "relation backlog-position-precedes exceeds target cardinality";
    assert !((source = BacklogPosition_0, target = BacklogPosition_1) in relation_backlog_position_precedes && (source = BacklogPosition_1, target = BacklogPosition_1) in relation_backlog_position_precedes), "relation backlog-position-precedes exceeds target cardinality";
    assert !((source = BacklogPosition_0, target = DeliveryTask_0) in relation_backlog_position_task) || (BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask), "relation backlog-position-task has an absent endpoint";
    assert !((source = BacklogPosition_0, target = DeliveryTask_1) in relation_backlog_position_task) || (BacklogPosition_0 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask), "relation backlog-position-task has an absent endpoint";
    assert !((source = BacklogPosition_1, target = DeliveryTask_0) in relation_backlog_position_task) || (BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_0 in exists_DeliveryTask), "relation backlog-position-task has an absent endpoint";
    assert !((source = BacklogPosition_1, target = DeliveryTask_1) in relation_backlog_position_task) || (BacklogPosition_1 in exists_BacklogPosition && DeliveryTask_1 in exists_DeliveryTask), "relation backlog-position-task has an absent endpoint";
    assert !((source = BacklogPosition_0, target = DeliveryTask_0) in relation_backlog_position_task && (source = BacklogPosition_0, target = DeliveryTask_1) in relation_backlog_position_task), "relation backlog-position-task exceeds source cardinality";
    assert !((source = BacklogPosition_1, target = DeliveryTask_0) in relation_backlog_position_task && (source = BacklogPosition_1, target = DeliveryTask_1) in relation_backlog_position_task), "relation backlog-position-task exceeds source cardinality";
    assert !((source = BacklogPosition_0, target = DeliveryTask_0) in relation_backlog_position_task && (source = BacklogPosition_1, target = DeliveryTask_0) in relation_backlog_position_task), "relation backlog-position-task exceeds target cardinality";
    assert !((source = BacklogPosition_0, target = DeliveryTask_1) in relation_backlog_position_task && (source = BacklogPosition_1, target = DeliveryTask_1) in relation_backlog_position_task), "relation backlog-position-task exceeds target cardinality";
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
    assert ((!(BacklogPosition_0 in exists_BacklogPosition) || ((!((state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = BacklogPosition_0, target = DeliveryTask_0) in relation_backlog_position_task && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = BacklogPosition_0, target = DeliveryTask_1) in relation_backlog_position_task && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired))))))))) && (!(BacklogPosition_1 in exists_BacklogPosition) || ((!((state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc)) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = BacklogPosition_1, target = DeliveryTask_0) in relation_backlog_position_task && (state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired)))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = BacklogPosition_1, target = DeliveryTask_1) in relation_backlog_position_task && (state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_acknowledged || state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))))), "property backlog.ack-after-dispatch failed";
    assert ((!(BacklogPosition_0 in exists_BacklogPosition) || (((!(BacklogPosition_0 in exists_BacklogPosition) || ((!(((source = BacklogPosition_0, target = BacklogPosition_0) in relation_backlog_position_precedes && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))) || ((state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))))) && (!(BacklogPosition_1 in exists_BacklogPosition) || ((!(((source = BacklogPosition_0, target = BacklogPosition_1) in relation_backlog_position_precedes && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc))) || ((state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc)))))))) && (!(BacklogPosition_1 in exists_BacklogPosition) || (((!(BacklogPosition_0 in exists_BacklogPosition) || ((!(((source = BacklogPosition_1, target = BacklogPosition_0) in relation_backlog_position_precedes && (state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc))) || ((state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc))))) && (!(BacklogPosition_1 in exists_BacklogPosition) || ((!(((source = BacklogPosition_1, target = BacklogPosition_1) in relation_backlog_position_precedes && (state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc))) || ((state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_acked || state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc))))))))), "property backlog.ack-prefix failed";
    assert ((!(BacklogPosition_0 in exists_BacklogPosition) || ((!(state_BacklogPosition[BacklogPosition_0] == BacklogPosition_state_gc) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = BacklogPosition_0, target = DeliveryTask_0) in relation_backlog_position_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = BacklogPosition_0, target = DeliveryTask_1) in relation_backlog_position_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired)))))))) && (!(BacklogPosition_1 in exists_BacklogPosition) || ((!(state_BacklogPosition[BacklogPosition_1] == BacklogPosition_state_gc) || (((DeliveryTask_0 in exists_DeliveryTask && (((source = BacklogPosition_1, target = DeliveryTask_0) in relation_backlog_position_task && state_DeliveryTask[DeliveryTask_0] == DeliveryTask_state_retired))) || (DeliveryTask_1 in exists_DeliveryTask && (((source = BacklogPosition_1, target = DeliveryTask_1) in relation_backlog_position_task && state_DeliveryTask[DeliveryTask_1] == DeliveryTask_state_retired))))))))), "property backlog.gc-after-retirement failed";
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
