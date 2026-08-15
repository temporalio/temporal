// Generated from the Umpire verification snapshot. Do not edit.

event eStep;

enum Callback { Callback_0, Callback_1 }
enum Callback_state { Callback_state_unobserved }
enum CallbackDelivery { CallbackDelivery_0, CallbackDelivery_1 }
enum CallbackDelivery_state { CallbackDelivery_state_acknowledged, CallbackDelivery_state_delivered, CallbackDelivery_state_failed, CallbackDelivery_state_pending }
enum CallbackResponse { CallbackResponse_0, CallbackResponse_1 }
enum CallbackResponse_state { CallbackResponse_state_accepted, CallbackResponse_state_conflicting, CallbackResponse_state_unobserved }
enum WorkflowRun { WorkflowRun_0, WorkflowRun_1 }
enum WorkflowRun_state { WorkflowRun_state_canceled, WorkflowRun_state_completed, WorkflowRun_state_continued_as_new, WorkflowRun_state_created, WorkflowRun_state_failed, WorkflowRun_state_started, WorkflowRun_state_terminated, WorkflowRun_state_timed_out }
type relation_callback_delivery_tuple = (source: Callback, target: CallbackDelivery);
type relation_callback_delivery_response_tuple = (source: CallbackDelivery, target: CallbackResponse);
type relation_callback_handler_run_tuple = (source: Callback, target: WorkflowRun);

machine UmpireWorld {
  var checkerStep: int;
  var exists_Callback: set[Callback];
  var state_Callback: map[Callback, Callback_state];
  var exists_CallbackDelivery: set[CallbackDelivery];
  var state_CallbackDelivery: map[CallbackDelivery, CallbackDelivery_state];
  var exists_CallbackResponse: set[CallbackResponse];
  var state_CallbackResponse: map[CallbackResponse, CallbackResponse_state];
  var exists_WorkflowRun: set[WorkflowRun];
  var state_WorkflowRun: map[WorkflowRun, WorkflowRun_state];
  var relation_callback_delivery: set[relation_callback_delivery_tuple];
  var relation_callback_delivery_response: set[relation_callback_delivery_response_tuple];
  var relation_callback_handler_run: set[relation_callback_handler_run_tuple];

  start state Init {
    entry {
      state_Callback[Callback_0] = Callback_state_unobserved;
      state_Callback[Callback_1] = Callback_state_unobserved;
      state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_pending;
      state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_pending;
      state_CallbackResponse[CallbackResponse_0] = CallbackResponse_state_unobserved;
      state_CallbackResponse[CallbackResponse_1] = CallbackResponse_state_unobserved;
      state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_created;
      state_WorkflowRun[WorkflowRun_1] = WorkflowRun_state_created;
      CheckSafety();
      send this, eStep;
    }
    on eStep do Step;
  }

  fun Step() {
    var enabled: set[int];
    if (!(Callback_0 in exists_Callback) && WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (0); }
    if (!(Callback_0 in exists_Callback) && WorkflowRun_1 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started) { enabled += (1); }
    if (!(Callback_1 in exists_Callback) && WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (2); }
    if (!(Callback_1 in exists_Callback) && WorkflowRun_1 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started) { enabled += (3); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && !(CallbackResponse_0 in exists_CallbackResponse) && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_delivered) { enabled += (4); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && !(CallbackResponse_1 in exists_CallbackResponse) && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_delivered) { enabled += (5); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && !(CallbackResponse_0 in exists_CallbackResponse) && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_delivered) { enabled += (6); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && !(CallbackResponse_1 in exists_CallbackResponse) && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_delivered) { enabled += (7); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_pending) { enabled += (8); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_pending) { enabled += (9); }
    if (Callback_0 in exists_Callback && !(CallbackDelivery_0 in exists_CallbackDelivery)) { enabled += (10); }
    if (Callback_0 in exists_Callback && !(CallbackDelivery_1 in exists_CallbackDelivery)) { enabled += (11); }
    if (Callback_1 in exists_Callback && !(CallbackDelivery_0 in exists_CallbackDelivery)) { enabled += (12); }
    if (Callback_1 in exists_Callback && !(CallbackDelivery_1 in exists_CallbackDelivery)) { enabled += (13); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_delivered) { enabled += (14); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_delivered) { enabled += (15); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_pending) { enabled += (16); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_pending) { enabled += (17); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_failed) { enabled += (18); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_failed) { enabled += (19); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (20); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (21); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (22); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (23); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (24); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (25); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (26); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (27); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (28); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (29); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (30); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (31); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created) { enabled += (32); }
    if (!(WorkflowRun_1 in exists_WorkflowRun) && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_created) { enabled += (33); }
    if (sizeof(enabled) == 0) {
      CheckQuiescent();
      raise halt;
    }
    if (0 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_handler_Callback_0_WorkflowRun_0(); return; }
      if ($) { Apply_callback_attach_handler_Callback_0_WorkflowRun_0(); return; }
      enabled -= (0);
    }
    if (1 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_handler_Callback_0_WorkflowRun_1(); return; }
      if ($) { Apply_callback_attach_handler_Callback_0_WorkflowRun_1(); return; }
      enabled -= (1);
    }
    if (2 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_handler_Callback_1_WorkflowRun_0(); return; }
      if ($) { Apply_callback_attach_handler_Callback_1_WorkflowRun_0(); return; }
      enabled -= (2);
    }
    if (3 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_handler_Callback_1_WorkflowRun_1(); return; }
      if ($) { Apply_callback_attach_handler_Callback_1_WorkflowRun_1(); return; }
      enabled -= (3);
    }
    if (4 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_0(); return; }
      if ($) { Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_0(); return; }
      enabled -= (4);
    }
    if (5 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_1(); return; }
      if ($) { Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_1(); return; }
      enabled -= (5);
    }
    if (6 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_0(); return; }
      if ($) { Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_0(); return; }
      enabled -= (6);
    }
    if (7 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_1(); return; }
      if ($) { Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_1(); return; }
      enabled -= (7);
    }
    if (8 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_deliver_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_deliver_CallbackDelivery_0(); return; }
      enabled -= (8);
    }
    if (9 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_deliver_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_deliver_CallbackDelivery_1(); return; }
      enabled -= (9);
    }
    if (10 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_0(); return; }
      enabled -= (10);
    }
    if (11 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_1(); return; }
      enabled -= (11);
    }
    if (12 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_0(); return; }
      enabled -= (12);
    }
    if (13 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_1(); return; }
      enabled -= (13);
    }
    if (14 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_fail_delivered_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_fail_delivered_CallbackDelivery_0(); return; }
      enabled -= (14);
    }
    if (15 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_fail_delivered_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_fail_delivered_CallbackDelivery_1(); return; }
      enabled -= (15);
    }
    if (16 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_fail_pending_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_fail_pending_CallbackDelivery_0(); return; }
      enabled -= (16);
    }
    if (17 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_fail_pending_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_fail_pending_CallbackDelivery_1(); return; }
      enabled -= (17);
    }
    if (18 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_retry_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_retry_CallbackDelivery_0(); return; }
      enabled -= (18);
    }
    if (19 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_retry_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_retry_CallbackDelivery_1(); return; }
      enabled -= (19);
    }
    if (20 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_cancel_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_cancel_WorkflowRun_0(); return; }
      enabled -= (20);
    }
    if (21 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_cancel_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_cancel_WorkflowRun_1(); return; }
      enabled -= (21);
    }
    if (22 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_complete_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_complete_WorkflowRun_0(); return; }
      enabled -= (22);
    }
    if (23 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_complete_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_complete_WorkflowRun_1(); return; }
      enabled -= (23);
    }
    if (24 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_continue_as_new_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_continue_as_new_WorkflowRun_0(); return; }
      enabled -= (24);
    }
    if (25 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_continue_as_new_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_continue_as_new_WorkflowRun_1(); return; }
      enabled -= (25);
    }
    if (26 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_fail_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_fail_WorkflowRun_0(); return; }
      enabled -= (26);
    }
    if (27 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_fail_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_fail_WorkflowRun_1(); return; }
      enabled -= (27);
    }
    if (28 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_terminate_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_terminate_WorkflowRun_0(); return; }
      enabled -= (28);
    }
    if (29 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_terminate_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_terminate_WorkflowRun_1(); return; }
      enabled -= (29);
    }
    if (30 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_timeout_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_timeout_WorkflowRun_0(); return; }
      enabled -= (30);
    }
    if (31 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_timeout_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_timeout_WorkflowRun_1(); return; }
      enabled -= (31);
    }
    if (32 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_start_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_start_WorkflowRun_0(); return; }
      enabled -= (32);
    }
    if (33 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_start_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_start_WorkflowRun_1(); return; }
      enabled -= (33);
    }
  }

  fun Apply_callback_attach_handler_Callback_0_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.attach-handler callback=Callback#0 handlerRun=WorkflowRun#0";
    exists_Callback += (Callback_0);
    state_Callback[Callback_0] = Callback_state_unobserved;
    relation_callback_handler_run += ((source = Callback_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_handler_Callback_0_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.attach-handler callback=Callback#0 handlerRun=WorkflowRun#1";
    exists_Callback += (Callback_0);
    state_Callback[Callback_0] = Callback_state_unobserved;
    relation_callback_handler_run += ((source = Callback_0, target = WorkflowRun_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_handler_Callback_1_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.attach-handler callback=Callback#1 handlerRun=WorkflowRun#0";
    exists_Callback += (Callback_1);
    state_Callback[Callback_1] = Callback_state_unobserved;
    relation_callback_handler_run += ((source = Callback_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_handler_Callback_1_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.attach-handler callback=Callback#1 handlerRun=WorkflowRun#1";
    exists_Callback += (Callback_1);
    state_Callback[Callback_1] = Callback_state_unobserved;
    relation_callback_handler_run += ((source = Callback_1, target = WorkflowRun_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_0() {
    print "UMPIRE_ACTION callback.delivery.acknowledge delivery=CallbackDelivery#0 response=CallbackResponse#0";
    state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_acknowledged;
    exists_CallbackResponse += (CallbackResponse_0);
    state_CallbackResponse[CallbackResponse_0] = CallbackResponse_state_accepted;
    relation_callback_delivery_response += ((source = CallbackDelivery_0, target = CallbackResponse_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_1() {
    print "UMPIRE_ACTION callback.delivery.acknowledge delivery=CallbackDelivery#0 response=CallbackResponse#1";
    state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_acknowledged;
    exists_CallbackResponse += (CallbackResponse_1);
    state_CallbackResponse[CallbackResponse_1] = CallbackResponse_state_accepted;
    relation_callback_delivery_response += ((source = CallbackDelivery_0, target = CallbackResponse_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_0() {
    print "UMPIRE_ACTION callback.delivery.acknowledge delivery=CallbackDelivery#1 response=CallbackResponse#0";
    state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_acknowledged;
    exists_CallbackResponse += (CallbackResponse_0);
    state_CallbackResponse[CallbackResponse_0] = CallbackResponse_state_accepted;
    relation_callback_delivery_response += ((source = CallbackDelivery_1, target = CallbackResponse_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_1() {
    print "UMPIRE_ACTION callback.delivery.acknowledge delivery=CallbackDelivery#1 response=CallbackResponse#1";
    state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_acknowledged;
    exists_CallbackResponse += (CallbackResponse_1);
    state_CallbackResponse[CallbackResponse_1] = CallbackResponse_state_accepted;
    relation_callback_delivery_response += ((source = CallbackDelivery_1, target = CallbackResponse_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_deliver_CallbackDelivery_0() {
    print "UMPIRE_ACTION callback.delivery.deliver delivery=CallbackDelivery#0";
    state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_delivered;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_deliver_CallbackDelivery_1() {
    print "UMPIRE_ACTION callback.delivery.deliver delivery=CallbackDelivery#1";
    state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_delivered;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_0() {
    print "UMPIRE_ACTION callback.delivery.enqueue callback=Callback#0 delivery=CallbackDelivery#0";
    exists_CallbackDelivery += (CallbackDelivery_0);
    state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_pending;
    relation_callback_delivery += ((source = Callback_0, target = CallbackDelivery_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_1() {
    print "UMPIRE_ACTION callback.delivery.enqueue callback=Callback#0 delivery=CallbackDelivery#1";
    exists_CallbackDelivery += (CallbackDelivery_1);
    state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_pending;
    relation_callback_delivery += ((source = Callback_0, target = CallbackDelivery_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_0() {
    print "UMPIRE_ACTION callback.delivery.enqueue callback=Callback#1 delivery=CallbackDelivery#0";
    exists_CallbackDelivery += (CallbackDelivery_0);
    state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_pending;
    relation_callback_delivery += ((source = Callback_1, target = CallbackDelivery_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_1() {
    print "UMPIRE_ACTION callback.delivery.enqueue callback=Callback#1 delivery=CallbackDelivery#1";
    exists_CallbackDelivery += (CallbackDelivery_1);
    state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_pending;
    relation_callback_delivery += ((source = Callback_1, target = CallbackDelivery_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_fail_delivered_CallbackDelivery_0() {
    print "UMPIRE_ACTION callback.delivery.fail-delivered delivery=CallbackDelivery#0";
    state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_fail_delivered_CallbackDelivery_1() {
    print "UMPIRE_ACTION callback.delivery.fail-delivered delivery=CallbackDelivery#1";
    state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_fail_pending_CallbackDelivery_0() {
    print "UMPIRE_ACTION callback.delivery.fail-pending delivery=CallbackDelivery#0";
    state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_fail_pending_CallbackDelivery_1() {
    print "UMPIRE_ACTION callback.delivery.fail-pending delivery=CallbackDelivery#1";
    state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_retry_CallbackDelivery_0() {
    print "UMPIRE_ACTION callback.delivery.retry delivery=CallbackDelivery#0";
    state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_pending;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_delivery_retry_CallbackDelivery_1() {
    print "UMPIRE_ACTION callback.delivery.retry delivery=CallbackDelivery#1";
    state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_pending;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_cancel_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.handler.close.cancel entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_cancel_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.handler.close.cancel entity=WorkflowRun#1";
    state_WorkflowRun[WorkflowRun_1] = WorkflowRun_state_canceled;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_complete_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.handler.close.complete entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_completed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_complete_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.handler.close.complete entity=WorkflowRun#1";
    state_WorkflowRun[WorkflowRun_1] = WorkflowRun_state_completed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_continue_as_new_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.handler.close.continue_as_new entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_continued_as_new;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_continue_as_new_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.handler.close.continue_as_new entity=WorkflowRun#1";
    state_WorkflowRun[WorkflowRun_1] = WorkflowRun_state_continued_as_new;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_fail_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.handler.close.fail entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_fail_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.handler.close.fail entity=WorkflowRun#1";
    state_WorkflowRun[WorkflowRun_1] = WorkflowRun_state_failed;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_terminate_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.handler.close.terminate entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_terminate_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.handler.close.terminate entity=WorkflowRun#1";
    state_WorkflowRun[WorkflowRun_1] = WorkflowRun_state_terminated;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_timeout_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.handler.close.timeout entity=WorkflowRun#0";
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_close_timeout_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.handler.close.timeout entity=WorkflowRun#1";
    state_WorkflowRun[WorkflowRun_1] = WorkflowRun_state_timed_out;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_start_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.handler.start entity=WorkflowRun#0";
    exists_WorkflowRun += (WorkflowRun_0);
    state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_started;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_handler_start_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.handler.start entity=WorkflowRun#1";
    exists_WorkflowRun += (WorkflowRun_1);
    state_WorkflowRun[WorkflowRun_1] = WorkflowRun_state_started;
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun CheckSafety() {
    assert !((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (Callback_0 in exists_Callback && CallbackDelivery_0 in exists_CallbackDelivery), "relation callback-delivery has an absent endpoint";
    assert !((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (Callback_0 in exists_Callback && CallbackDelivery_1 in exists_CallbackDelivery), "relation callback-delivery has an absent endpoint";
    assert !((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (Callback_1 in exists_Callback && CallbackDelivery_0 in exists_CallbackDelivery), "relation callback-delivery has an absent endpoint";
    assert !((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (Callback_1 in exists_Callback && CallbackDelivery_1 in exists_CallbackDelivery), "relation callback-delivery has an absent endpoint";
    assert !((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery && (source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery), "relation callback-delivery exceeds target cardinality";
    assert !((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery && (source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery), "relation callback-delivery exceeds target cardinality";
    assert !((source = CallbackDelivery_0, target = CallbackResponse_0) in relation_callback_delivery_response) || (CallbackDelivery_0 in exists_CallbackDelivery && CallbackResponse_0 in exists_CallbackResponse), "relation callback-delivery-response has an absent endpoint";
    assert !((source = CallbackDelivery_0, target = CallbackResponse_1) in relation_callback_delivery_response) || (CallbackDelivery_0 in exists_CallbackDelivery && CallbackResponse_1 in exists_CallbackResponse), "relation callback-delivery-response has an absent endpoint";
    assert !((source = CallbackDelivery_1, target = CallbackResponse_0) in relation_callback_delivery_response) || (CallbackDelivery_1 in exists_CallbackDelivery && CallbackResponse_0 in exists_CallbackResponse), "relation callback-delivery-response has an absent endpoint";
    assert !((source = CallbackDelivery_1, target = CallbackResponse_1) in relation_callback_delivery_response) || (CallbackDelivery_1 in exists_CallbackDelivery && CallbackResponse_1 in exists_CallbackResponse), "relation callback-delivery-response has an absent endpoint";
    assert !((source = CallbackDelivery_0, target = CallbackResponse_0) in relation_callback_delivery_response && (source = CallbackDelivery_0, target = CallbackResponse_1) in relation_callback_delivery_response), "relation callback-delivery-response exceeds source cardinality";
    assert !((source = CallbackDelivery_1, target = CallbackResponse_0) in relation_callback_delivery_response && (source = CallbackDelivery_1, target = CallbackResponse_1) in relation_callback_delivery_response), "relation callback-delivery-response exceeds source cardinality";
    assert !((source = CallbackDelivery_0, target = CallbackResponse_0) in relation_callback_delivery_response && (source = CallbackDelivery_1, target = CallbackResponse_0) in relation_callback_delivery_response), "relation callback-delivery-response exceeds target cardinality";
    assert !((source = CallbackDelivery_0, target = CallbackResponse_1) in relation_callback_delivery_response && (source = CallbackDelivery_1, target = CallbackResponse_1) in relation_callback_delivery_response), "relation callback-delivery-response exceeds target cardinality";
    assert !((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (Callback_0 in exists_Callback && WorkflowRun_0 in exists_WorkflowRun), "relation callback-handler-run has an absent endpoint";
    assert !((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (Callback_0 in exists_Callback && WorkflowRun_1 in exists_WorkflowRun), "relation callback-handler-run has an absent endpoint";
    assert !((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (Callback_1 in exists_Callback && WorkflowRun_0 in exists_WorkflowRun), "relation callback-handler-run has an absent endpoint";
    assert !((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (Callback_1 in exists_Callback && WorkflowRun_1 in exists_WorkflowRun), "relation callback-handler-run has an absent endpoint";
    assert !((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run && (source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run), "relation callback-handler-run exceeds source cardinality";
    assert !((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run && (source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run), "relation callback-handler-run exceeds source cardinality";
    assert ((!(WorkflowRun_0 in exists_WorkflowRun) || ((!((state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_completed || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_failed || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_canceled || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_terminated || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_timed_out || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_continued_as_new)) || (((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))))) && (!(WorkflowRun_1 in exists_WorkflowRun) || ((!((state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_completed || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_failed || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_canceled || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_terminated || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_timed_out || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_continued_as_new)) || (((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))))))))), "property CallbackHandlerLifetime failed";
    assert (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!(state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged) || (((CallbackResponse_0 in exists_CallbackResponse && (((source = CallbackDelivery_0, target = CallbackResponse_0) in relation_callback_delivery_response && state_CallbackResponse[CallbackResponse_0] == CallbackResponse_state_accepted))) || (CallbackResponse_1 in exists_CallbackResponse && (((source = CallbackDelivery_0, target = CallbackResponse_1) in relation_callback_delivery_response && state_CallbackResponse[CallbackResponse_1] == CallbackResponse_state_accepted)))))))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!(state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged) || (((CallbackResponse_0 in exists_CallbackResponse && (((source = CallbackDelivery_1, target = CallbackResponse_0) in relation_callback_delivery_response && state_CallbackResponse[CallbackResponse_0] == CallbackResponse_state_accepted))) || (CallbackResponse_1 in exists_CallbackResponse && (((source = CallbackDelivery_1, target = CallbackResponse_1) in relation_callback_delivery_response && state_CallbackResponse[CallbackResponse_1] == CallbackResponse_state_accepted))))))))) && ((!(CallbackDelivery_0 in exists_CallbackDelivery) || (((!(CallbackResponse_0 in exists_CallbackResponse) || ((!((source = CallbackDelivery_0, target = CallbackResponse_0) in relation_callback_delivery_response) || (state_CallbackResponse[CallbackResponse_0] == CallbackResponse_state_accepted)))) && (!(CallbackResponse_1 in exists_CallbackResponse) || ((!((source = CallbackDelivery_0, target = CallbackResponse_1) in relation_callback_delivery_response) || (state_CallbackResponse[CallbackResponse_1] == CallbackResponse_state_accepted))))))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || (((!(CallbackResponse_0 in exists_CallbackResponse) || ((!((source = CallbackDelivery_1, target = CallbackResponse_0) in relation_callback_delivery_response) || (state_CallbackResponse[CallbackResponse_0] == CallbackResponse_state_accepted)))) && (!(CallbackResponse_1 in exists_CallbackResponse) || ((!((source = CallbackDelivery_1, target = CallbackResponse_1) in relation_callback_delivery_response) || (state_CallbackResponse[CallbackResponse_1] == CallbackResponse_state_accepted))))))))), "property CallbackResponseConsistency failed";
  }

  fun CheckQuiescent() {
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
