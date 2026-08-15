// Generated from the Umpire verification snapshot. Do not edit.

event eStep;

enum Callback { Callback_0, Callback_1 }
enum Callback_state { Callback_state_unobserved }
enum CallbackDelivery { CallbackDelivery_0, CallbackDelivery_1 }
enum CallbackDelivery_state { CallbackDelivery_state_acknowledged, CallbackDelivery_state_delivered, CallbackDelivery_state_failed, CallbackDelivery_state_pending }
enum CallbackResponse { CallbackResponse_0, CallbackResponse_1 }
enum CallbackResponse_state { CallbackResponse_state_accepted, CallbackResponse_state_conflicting, CallbackResponse_state_unobserved }
enum NexusOperation { NexusOperation_0, NexusOperation_1 }
enum NexusOperation_state { NexusOperation_state_backing_off, NexusOperation_state_canceled, NexusOperation_state_failed, NexusOperation_state_rejected, NexusOperation_state_scheduled, NexusOperation_state_started, NexusOperation_state_succeeded, NexusOperation_state_terminated, NexusOperation_state_timed_out, NexusOperation_state_unspecified }
enum WorkflowRun { WorkflowRun_0, WorkflowRun_1 }
enum WorkflowRun_state { WorkflowRun_state_canceled, WorkflowRun_state_completed, WorkflowRun_state_continued_as_new, WorkflowRun_state_created, WorkflowRun_state_failed, WorkflowRun_state_started, WorkflowRun_state_terminated, WorkflowRun_state_timed_out }
type relation_callback_delivery_tuple = (source: Callback, target: CallbackDelivery);
type relation_callback_delivery_response_tuple = (source: CallbackDelivery, target: CallbackResponse);
type relation_callback_handler_run_tuple = (source: Callback, target: WorkflowRun);
type relation_callback_operation_tuple = (source: Callback, target: NexusOperation);
type relation_nexus_operation_handler_run_tuple = (source: NexusOperation, target: WorkflowRun);

machine UmpireWorld {
  var checkerStep: int;
  var exists_Callback: set[Callback];
  var state_Callback: map[Callback, Callback_state];
  var exists_CallbackDelivery: set[CallbackDelivery];
  var state_CallbackDelivery: map[CallbackDelivery, CallbackDelivery_state];
  var exists_CallbackResponse: set[CallbackResponse];
  var state_CallbackResponse: map[CallbackResponse, CallbackResponse_state];
  var exists_NexusOperation: set[NexusOperation];
  var state_NexusOperation: map[NexusOperation, NexusOperation_state];
  var exists_WorkflowRun: set[WorkflowRun];
  var state_WorkflowRun: map[WorkflowRun, WorkflowRun_state];
  var relation_callback_delivery: set[relation_callback_delivery_tuple];
  var relation_callback_delivery_response: set[relation_callback_delivery_response_tuple];
  var relation_callback_handler_run: set[relation_callback_handler_run_tuple];
  var relation_callback_operation: set[relation_callback_operation_tuple];
  var relation_nexus_operation_handler_run: set[relation_nexus_operation_handler_run_tuple];

  start state Init {
    entry {
      state_Callback[Callback_0] = Callback_state_unobserved;
      state_Callback[Callback_1] = Callback_state_unobserved;
      state_CallbackDelivery[CallbackDelivery_0] = CallbackDelivery_state_pending;
      state_CallbackDelivery[CallbackDelivery_1] = CallbackDelivery_state_pending;
      state_CallbackResponse[CallbackResponse_0] = CallbackResponse_state_unobserved;
      state_CallbackResponse[CallbackResponse_1] = CallbackResponse_state_unobserved;
      state_NexusOperation[NexusOperation_0] = NexusOperation_state_unspecified;
      state_NexusOperation[NexusOperation_1] = NexusOperation_state_unspecified;
      state_WorkflowRun[WorkflowRun_0] = WorkflowRun_state_created;
      state_WorkflowRun[WorkflowRun_1] = WorkflowRun_state_created;
      CheckSafety();
      send this, eStep;
    }
    on eStep do Step;
  }

  fun Step() {
    var enabled: set[int];
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (0); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (1); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (2); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (3); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (4); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (5); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (6); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (7); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (8); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (9); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_backing_off) { enabled += (10); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_backing_off) { enabled += (11); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (12); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (13); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (14); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (15); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (16); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (17); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (18); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (19); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (20); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (21); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (22); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (23); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (24); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (25); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (26); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (27); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (28); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (29); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (30); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (31); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (32); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (33); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (34); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (35); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (36); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (37); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_scheduled) { enabled += (38); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_scheduled) { enabled += (39); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (40); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (41); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (42); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (43); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (44); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (45); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (46); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (47); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (48); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (49); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (50); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (51); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (52); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (53); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (54); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (55); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (56); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (57); }
    if (NexusOperation_0 in exists_NexusOperation && state_NexusOperation[NexusOperation_0] == NexusOperation_state_started) { enabled += (58); }
    if (NexusOperation_1 in exists_NexusOperation && state_NexusOperation[NexusOperation_1] == NexusOperation_state_started) { enabled += (59); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (60); }
    if (!(NexusOperation_1 in exists_NexusOperation) && state_NexusOperation[NexusOperation_1] == NexusOperation_state_unspecified) { enabled += (61); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (62); }
    if (!(NexusOperation_1 in exists_NexusOperation) && state_NexusOperation[NexusOperation_1] == NexusOperation_state_unspecified) { enabled += (63); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (64); }
    if (!(NexusOperation_1 in exists_NexusOperation) && state_NexusOperation[NexusOperation_1] == NexusOperation_state_unspecified) { enabled += (65); }
    if (!(NexusOperation_0 in exists_NexusOperation) && state_NexusOperation[NexusOperation_0] == NexusOperation_state_unspecified) { enabled += (66); }
    if (!(NexusOperation_1 in exists_NexusOperation) && state_NexusOperation[NexusOperation_1] == NexusOperation_state_unspecified) { enabled += (67); }
    if (!(Callback_0 in exists_Callback) && WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (68); }
    if (!(Callback_0 in exists_Callback) && WorkflowRun_1 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started) { enabled += (69); }
    if (!(Callback_1 in exists_Callback) && WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (70); }
    if (!(Callback_1 in exists_Callback) && WorkflowRun_1 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started) { enabled += (71); }
    if (!(Callback_0 in exists_Callback) && NexusOperation_0 in exists_NexusOperation && WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (72); }
    if (!(Callback_0 in exists_Callback) && NexusOperation_0 in exists_NexusOperation && WorkflowRun_1 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started) { enabled += (73); }
    if (!(Callback_0 in exists_Callback) && NexusOperation_1 in exists_NexusOperation && WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (74); }
    if (!(Callback_0 in exists_Callback) && NexusOperation_1 in exists_NexusOperation && WorkflowRun_1 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started) { enabled += (75); }
    if (!(Callback_1 in exists_Callback) && NexusOperation_0 in exists_NexusOperation && WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (76); }
    if (!(Callback_1 in exists_Callback) && NexusOperation_0 in exists_NexusOperation && WorkflowRun_1 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started) { enabled += (77); }
    if (!(Callback_1 in exists_Callback) && NexusOperation_1 in exists_NexusOperation && WorkflowRun_0 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started) { enabled += (78); }
    if (!(Callback_1 in exists_Callback) && NexusOperation_1 in exists_NexusOperation && WorkflowRun_1 in exists_WorkflowRun && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started) { enabled += (79); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && !(CallbackResponse_0 in exists_CallbackResponse) && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_delivered) { enabled += (80); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && !(CallbackResponse_1 in exists_CallbackResponse) && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_delivered) { enabled += (81); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && !(CallbackResponse_0 in exists_CallbackResponse) && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_delivered) { enabled += (82); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && !(CallbackResponse_1 in exists_CallbackResponse) && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_delivered) { enabled += (83); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_pending) { enabled += (84); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_pending) { enabled += (85); }
    if (Callback_0 in exists_Callback && !(CallbackDelivery_0 in exists_CallbackDelivery)) { enabled += (86); }
    if (Callback_0 in exists_Callback && !(CallbackDelivery_1 in exists_CallbackDelivery)) { enabled += (87); }
    if (Callback_1 in exists_Callback && !(CallbackDelivery_0 in exists_CallbackDelivery)) { enabled += (88); }
    if (Callback_1 in exists_Callback && !(CallbackDelivery_1 in exists_CallbackDelivery)) { enabled += (89); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_delivered) { enabled += (90); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_delivered) { enabled += (91); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_pending) { enabled += (92); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_pending) { enabled += (93); }
    if (CallbackDelivery_0 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_failed) { enabled += (94); }
    if (CallbackDelivery_1 in exists_CallbackDelivery && state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_failed) { enabled += (95); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (96); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (97); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (98); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (99); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (100); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (101); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (102); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (103); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (104); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (105); }
    if (WorkflowRun_0 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (106); }
    if (WorkflowRun_1 in exists_WorkflowRun && (state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_started && ((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))) { enabled += (107); }
    if (!(WorkflowRun_0 in exists_WorkflowRun) && state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_created) { enabled += (108); }
    if (!(WorkflowRun_1 in exists_WorkflowRun) && state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_created) { enabled += (109); }
    if (sizeof(enabled) == 0) {
      CheckQuiescent();
      raise halt;
    }
    if (0 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_0(); return; }
      enabled -= (0);
    }
    if (1 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_schedule_Embedded_NexusOperation_1(); return; }
      enabled -= (1);
    }
    if (2 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_0(); return; }
      enabled -= (2);
    }
    if (3 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_schedule_Standalone_NexusOperation_1(); return; }
      enabled -= (3);
    }
    if (4 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_0(); return; }
      enabled -= (4);
    }
    if (5 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_terminate_Embedded_NexusOperation_1(); return; }
      enabled -= (5);
    }
    if (6 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_0(); return; }
      enabled -= (6);
    }
    if (7 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_terminate_Standalone_NexusOperation_1(); return; }
      enabled -= (7);
    }
    if (8 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_0(); return; }
      enabled -= (8);
    }
    if (9 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_timeout_Embedded_NexusOperation_1(); return; }
      enabled -= (9);
    }
    if (10 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_0(); return; }
      enabled -= (10);
    }
    if (11 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_backing_off_timeout_Standalone_NexusOperation_1(); return; }
      enabled -= (11);
    }
    if (12 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_0(); return; }
      enabled -= (12);
    }
    if (13 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_attempt_failed_Embedded_NexusOperation_1(); return; }
      enabled -= (13);
    }
    if (14 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_0(); return; }
      enabled -= (14);
    }
    if (15 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_attempt_failed_Standalone_NexusOperation_1(); return; }
      enabled -= (15);
    }
    if (16 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_0(); return; }
      enabled -= (16);
    }
    if (17 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_cancel_Embedded_NexusOperation_1(); return; }
      enabled -= (17);
    }
    if (18 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_0(); return; }
      enabled -= (18);
    }
    if (19 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_cancel_Standalone_NexusOperation_1(); return; }
      enabled -= (19);
    }
    if (20 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_0(); return; }
      enabled -= (20);
    }
    if (21 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_fail_Embedded_NexusOperation_1(); return; }
      enabled -= (21);
    }
    if (22 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_0(); return; }
      enabled -= (22);
    }
    if (23 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_fail_Standalone_NexusOperation_1(); return; }
      enabled -= (23);
    }
    if (24 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_0(); return; }
      enabled -= (24);
    }
    if (25 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_start_Embedded_NexusOperation_1(); return; }
      enabled -= (25);
    }
    if (26 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_0(); return; }
      enabled -= (26);
    }
    if (27 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_start_Standalone_NexusOperation_1(); return; }
      enabled -= (27);
    }
    if (28 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_0(); return; }
      enabled -= (28);
    }
    if (29 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_succeed_Embedded_NexusOperation_1(); return; }
      enabled -= (29);
    }
    if (30 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_0(); return; }
      enabled -= (30);
    }
    if (31 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_succeed_Standalone_NexusOperation_1(); return; }
      enabled -= (31);
    }
    if (32 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_0(); return; }
      enabled -= (32);
    }
    if (33 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_terminate_Embedded_NexusOperation_1(); return; }
      enabled -= (33);
    }
    if (34 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_0(); return; }
      enabled -= (34);
    }
    if (35 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_terminate_Standalone_NexusOperation_1(); return; }
      enabled -= (35);
    }
    if (36 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_0(); return; }
      enabled -= (36);
    }
    if (37 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_timeout_Embedded_NexusOperation_1(); return; }
      enabled -= (37);
    }
    if (38 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_0(); return; }
      enabled -= (38);
    }
    if (39 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_scheduled_timeout_Standalone_NexusOperation_1(); return; }
      enabled -= (39);
    }
    if (40 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_0(); return; }
      enabled -= (40);
    }
    if (41 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_cancel_Embedded_NexusOperation_1(); return; }
      enabled -= (41);
    }
    if (42 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_0(); return; }
      enabled -= (42);
    }
    if (43 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_cancel_Standalone_NexusOperation_1(); return; }
      enabled -= (43);
    }
    if (44 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_0(); return; }
      enabled -= (44);
    }
    if (45 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_fail_Embedded_NexusOperation_1(); return; }
      enabled -= (45);
    }
    if (46 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_0(); return; }
      enabled -= (46);
    }
    if (47 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_fail_Standalone_NexusOperation_1(); return; }
      enabled -= (47);
    }
    if (48 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_0(); return; }
      enabled -= (48);
    }
    if (49 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_succeed_Embedded_NexusOperation_1(); return; }
      enabled -= (49);
    }
    if (50 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_0(); return; }
      enabled -= (50);
    }
    if (51 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_succeed_Standalone_NexusOperation_1(); return; }
      enabled -= (51);
    }
    if (52 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_0(); return; }
      enabled -= (52);
    }
    if (53 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_terminate_Embedded_NexusOperation_1(); return; }
      enabled -= (53);
    }
    if (54 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_0(); return; }
      enabled -= (54);
    }
    if (55 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_terminate_Standalone_NexusOperation_1(); return; }
      enabled -= (55);
    }
    if (56 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_0(); return; }
      enabled -= (56);
    }
    if (57 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_timeout_Embedded_NexusOperation_1(); return; }
      enabled -= (57);
    }
    if (58 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_0(); return; }
      enabled -= (58);
    }
    if (59 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_started_timeout_Standalone_NexusOperation_1(); return; }
      enabled -= (59);
    }
    if (60 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_0(); return; }
      enabled -= (60);
    }
    if (61 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_unspecified_reject_Embedded_NexusOperation_1(); return; }
      enabled -= (61);
    }
    if (62 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_0(); return; }
      enabled -= (62);
    }
    if (63 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_unspecified_reject_Standalone_NexusOperation_1(); return; }
      enabled -= (63);
    }
    if (64 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_0(); return; }
      enabled -= (64);
    }
    if (65 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_unspecified_schedule_Embedded_NexusOperation_1(); return; }
      enabled -= (65);
    }
    if (66 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_0(); return; }
      if ($) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_0(); return; }
      enabled -= (66);
    }
    if (67 in enabled) {
      if (sizeof(enabled) == 1) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_1(); return; }
      if ($) { Apply_NexusOperation_unspecified_schedule_Standalone_NexusOperation_1(); return; }
      enabled -= (67);
    }
    if (68 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_handler_Callback_0_WorkflowRun_0(); return; }
      if ($) { Apply_callback_attach_handler_Callback_0_WorkflowRun_0(); return; }
      enabled -= (68);
    }
    if (69 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_handler_Callback_0_WorkflowRun_1(); return; }
      if ($) { Apply_callback_attach_handler_Callback_0_WorkflowRun_1(); return; }
      enabled -= (69);
    }
    if (70 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_handler_Callback_1_WorkflowRun_0(); return; }
      if ($) { Apply_callback_attach_handler_Callback_1_WorkflowRun_0(); return; }
      enabled -= (70);
    }
    if (71 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_handler_Callback_1_WorkflowRun_1(); return; }
      if ($) { Apply_callback_attach_handler_Callback_1_WorkflowRun_1(); return; }
      enabled -= (71);
    }
    if (72 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_reference_Callback_0_NexusOperation_0_WorkflowRun_0(); return; }
      if ($) { Apply_callback_attach_reference_Callback_0_NexusOperation_0_WorkflowRun_0(); return; }
      enabled -= (72);
    }
    if (73 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_reference_Callback_0_NexusOperation_0_WorkflowRun_1(); return; }
      if ($) { Apply_callback_attach_reference_Callback_0_NexusOperation_0_WorkflowRun_1(); return; }
      enabled -= (73);
    }
    if (74 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_reference_Callback_0_NexusOperation_1_WorkflowRun_0(); return; }
      if ($) { Apply_callback_attach_reference_Callback_0_NexusOperation_1_WorkflowRun_0(); return; }
      enabled -= (74);
    }
    if (75 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_reference_Callback_0_NexusOperation_1_WorkflowRun_1(); return; }
      if ($) { Apply_callback_attach_reference_Callback_0_NexusOperation_1_WorkflowRun_1(); return; }
      enabled -= (75);
    }
    if (76 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_reference_Callback_1_NexusOperation_0_WorkflowRun_0(); return; }
      if ($) { Apply_callback_attach_reference_Callback_1_NexusOperation_0_WorkflowRun_0(); return; }
      enabled -= (76);
    }
    if (77 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_reference_Callback_1_NexusOperation_0_WorkflowRun_1(); return; }
      if ($) { Apply_callback_attach_reference_Callback_1_NexusOperation_0_WorkflowRun_1(); return; }
      enabled -= (77);
    }
    if (78 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_reference_Callback_1_NexusOperation_1_WorkflowRun_0(); return; }
      if ($) { Apply_callback_attach_reference_Callback_1_NexusOperation_1_WorkflowRun_0(); return; }
      enabled -= (78);
    }
    if (79 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_attach_reference_Callback_1_NexusOperation_1_WorkflowRun_1(); return; }
      if ($) { Apply_callback_attach_reference_Callback_1_NexusOperation_1_WorkflowRun_1(); return; }
      enabled -= (79);
    }
    if (80 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_0(); return; }
      if ($) { Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_0(); return; }
      enabled -= (80);
    }
    if (81 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_1(); return; }
      if ($) { Apply_callback_delivery_acknowledge_CallbackDelivery_0_CallbackResponse_1(); return; }
      enabled -= (81);
    }
    if (82 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_0(); return; }
      if ($) { Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_0(); return; }
      enabled -= (82);
    }
    if (83 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_1(); return; }
      if ($) { Apply_callback_delivery_acknowledge_CallbackDelivery_1_CallbackResponse_1(); return; }
      enabled -= (83);
    }
    if (84 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_deliver_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_deliver_CallbackDelivery_0(); return; }
      enabled -= (84);
    }
    if (85 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_deliver_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_deliver_CallbackDelivery_1(); return; }
      enabled -= (85);
    }
    if (86 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_0(); return; }
      enabled -= (86);
    }
    if (87 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_enqueue_Callback_0_CallbackDelivery_1(); return; }
      enabled -= (87);
    }
    if (88 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_0(); return; }
      enabled -= (88);
    }
    if (89 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_enqueue_Callback_1_CallbackDelivery_1(); return; }
      enabled -= (89);
    }
    if (90 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_fail_delivered_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_fail_delivered_CallbackDelivery_0(); return; }
      enabled -= (90);
    }
    if (91 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_fail_delivered_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_fail_delivered_CallbackDelivery_1(); return; }
      enabled -= (91);
    }
    if (92 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_fail_pending_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_fail_pending_CallbackDelivery_0(); return; }
      enabled -= (92);
    }
    if (93 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_fail_pending_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_fail_pending_CallbackDelivery_1(); return; }
      enabled -= (93);
    }
    if (94 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_retry_CallbackDelivery_0(); return; }
      if ($) { Apply_callback_delivery_retry_CallbackDelivery_0(); return; }
      enabled -= (94);
    }
    if (95 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_delivery_retry_CallbackDelivery_1(); return; }
      if ($) { Apply_callback_delivery_retry_CallbackDelivery_1(); return; }
      enabled -= (95);
    }
    if (96 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_cancel_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_cancel_WorkflowRun_0(); return; }
      enabled -= (96);
    }
    if (97 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_cancel_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_cancel_WorkflowRun_1(); return; }
      enabled -= (97);
    }
    if (98 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_complete_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_complete_WorkflowRun_0(); return; }
      enabled -= (98);
    }
    if (99 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_complete_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_complete_WorkflowRun_1(); return; }
      enabled -= (99);
    }
    if (100 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_continue_as_new_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_continue_as_new_WorkflowRun_0(); return; }
      enabled -= (100);
    }
    if (101 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_continue_as_new_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_continue_as_new_WorkflowRun_1(); return; }
      enabled -= (101);
    }
    if (102 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_fail_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_fail_WorkflowRun_0(); return; }
      enabled -= (102);
    }
    if (103 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_fail_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_fail_WorkflowRun_1(); return; }
      enabled -= (103);
    }
    if (104 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_terminate_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_terminate_WorkflowRun_0(); return; }
      enabled -= (104);
    }
    if (105 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_terminate_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_terminate_WorkflowRun_1(); return; }
      enabled -= (105);
    }
    if (106 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_timeout_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_close_timeout_WorkflowRun_0(); return; }
      enabled -= (106);
    }
    if (107 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_close_timeout_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_close_timeout_WorkflowRun_1(); return; }
      enabled -= (107);
    }
    if (108 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_start_WorkflowRun_0(); return; }
      if ($) { Apply_callback_handler_start_WorkflowRun_0(); return; }
      enabled -= (108);
    }
    if (109 in enabled) {
      if (sizeof(enabled) == 1) { Apply_callback_handler_start_WorkflowRun_1(); return; }
      if ($) { Apply_callback_handler_start_WorkflowRun_1(); return; }
      enabled -= (109);
    }
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

  fun Apply_callback_attach_reference_Callback_0_NexusOperation_0_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.attach-reference callback=Callback#0 operation=NexusOperation#0 handlerRun=WorkflowRun#0";
    exists_Callback += (Callback_0);
    state_Callback[Callback_0] = Callback_state_unobserved;
    relation_callback_operation += ((source = Callback_0, target = NexusOperation_0));
    relation_callback_handler_run += ((source = Callback_0, target = WorkflowRun_0));
    relation_nexus_operation_handler_run += ((source = NexusOperation_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_reference_Callback_0_NexusOperation_0_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.attach-reference callback=Callback#0 operation=NexusOperation#0 handlerRun=WorkflowRun#1";
    exists_Callback += (Callback_0);
    state_Callback[Callback_0] = Callback_state_unobserved;
    relation_callback_operation += ((source = Callback_0, target = NexusOperation_0));
    relation_callback_handler_run += ((source = Callback_0, target = WorkflowRun_1));
    relation_nexus_operation_handler_run += ((source = NexusOperation_0, target = WorkflowRun_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_reference_Callback_0_NexusOperation_1_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.attach-reference callback=Callback#0 operation=NexusOperation#1 handlerRun=WorkflowRun#0";
    exists_Callback += (Callback_0);
    state_Callback[Callback_0] = Callback_state_unobserved;
    relation_callback_operation += ((source = Callback_0, target = NexusOperation_1));
    relation_callback_handler_run += ((source = Callback_0, target = WorkflowRun_0));
    relation_nexus_operation_handler_run += ((source = NexusOperation_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_reference_Callback_0_NexusOperation_1_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.attach-reference callback=Callback#0 operation=NexusOperation#1 handlerRun=WorkflowRun#1";
    exists_Callback += (Callback_0);
    state_Callback[Callback_0] = Callback_state_unobserved;
    relation_callback_operation += ((source = Callback_0, target = NexusOperation_1));
    relation_callback_handler_run += ((source = Callback_0, target = WorkflowRun_1));
    relation_nexus_operation_handler_run += ((source = NexusOperation_1, target = WorkflowRun_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_reference_Callback_1_NexusOperation_0_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.attach-reference callback=Callback#1 operation=NexusOperation#0 handlerRun=WorkflowRun#0";
    exists_Callback += (Callback_1);
    state_Callback[Callback_1] = Callback_state_unobserved;
    relation_callback_operation += ((source = Callback_1, target = NexusOperation_0));
    relation_callback_handler_run += ((source = Callback_1, target = WorkflowRun_0));
    relation_nexus_operation_handler_run += ((source = NexusOperation_0, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_reference_Callback_1_NexusOperation_0_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.attach-reference callback=Callback#1 operation=NexusOperation#0 handlerRun=WorkflowRun#1";
    exists_Callback += (Callback_1);
    state_Callback[Callback_1] = Callback_state_unobserved;
    relation_callback_operation += ((source = Callback_1, target = NexusOperation_0));
    relation_callback_handler_run += ((source = Callback_1, target = WorkflowRun_1));
    relation_nexus_operation_handler_run += ((source = NexusOperation_0, target = WorkflowRun_1));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_reference_Callback_1_NexusOperation_1_WorkflowRun_0() {
    print "UMPIRE_ACTION callback.attach-reference callback=Callback#1 operation=NexusOperation#1 handlerRun=WorkflowRun#0";
    exists_Callback += (Callback_1);
    state_Callback[Callback_1] = Callback_state_unobserved;
    relation_callback_operation += ((source = Callback_1, target = NexusOperation_1));
    relation_callback_handler_run += ((source = Callback_1, target = WorkflowRun_0));
    relation_nexus_operation_handler_run += ((source = NexusOperation_1, target = WorkflowRun_0));
    CheckSafety();
    checkerStep = checkerStep + 1;
    send this, eStep;
  }

  fun Apply_callback_attach_reference_Callback_1_NexusOperation_1_WorkflowRun_1() {
    print "UMPIRE_ACTION callback.attach-reference callback=Callback#1 operation=NexusOperation#1 handlerRun=WorkflowRun#1";
    exists_Callback += (Callback_1);
    state_Callback[Callback_1] = Callback_state_unobserved;
    relation_callback_operation += ((source = Callback_1, target = NexusOperation_1));
    relation_callback_handler_run += ((source = Callback_1, target = WorkflowRun_1));
    relation_nexus_operation_handler_run += ((source = NexusOperation_1, target = WorkflowRun_1));
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
    assert !((source = Callback_0, target = NexusOperation_0) in relation_callback_operation) || (Callback_0 in exists_Callback && NexusOperation_0 in exists_NexusOperation), "relation callback-operation has an absent endpoint";
    assert !((source = Callback_0, target = NexusOperation_1) in relation_callback_operation) || (Callback_0 in exists_Callback && NexusOperation_1 in exists_NexusOperation), "relation callback-operation has an absent endpoint";
    assert !((source = Callback_1, target = NexusOperation_0) in relation_callback_operation) || (Callback_1 in exists_Callback && NexusOperation_0 in exists_NexusOperation), "relation callback-operation has an absent endpoint";
    assert !((source = Callback_1, target = NexusOperation_1) in relation_callback_operation) || (Callback_1 in exists_Callback && NexusOperation_1 in exists_NexusOperation), "relation callback-operation has an absent endpoint";
    assert !((source = Callback_0, target = NexusOperation_0) in relation_callback_operation && (source = Callback_0, target = NexusOperation_1) in relation_callback_operation), "relation callback-operation exceeds source cardinality";
    assert !((source = Callback_1, target = NexusOperation_0) in relation_callback_operation && (source = Callback_1, target = NexusOperation_1) in relation_callback_operation), "relation callback-operation exceeds source cardinality";
    assert !((source = NexusOperation_0, target = WorkflowRun_0) in relation_nexus_operation_handler_run) || (NexusOperation_0 in exists_NexusOperation && WorkflowRun_0 in exists_WorkflowRun), "relation nexus-operation-handler-run has an absent endpoint";
    assert !((source = NexusOperation_0, target = WorkflowRun_1) in relation_nexus_operation_handler_run) || (NexusOperation_0 in exists_NexusOperation && WorkflowRun_1 in exists_WorkflowRun), "relation nexus-operation-handler-run has an absent endpoint";
    assert !((source = NexusOperation_1, target = WorkflowRun_0) in relation_nexus_operation_handler_run) || (NexusOperation_1 in exists_NexusOperation && WorkflowRun_0 in exists_WorkflowRun), "relation nexus-operation-handler-run has an absent endpoint";
    assert !((source = NexusOperation_1, target = WorkflowRun_1) in relation_nexus_operation_handler_run) || (NexusOperation_1 in exists_NexusOperation && WorkflowRun_1 in exists_WorkflowRun), "relation nexus-operation-handler-run has an absent endpoint";
    assert !((source = NexusOperation_0, target = WorkflowRun_0) in relation_nexus_operation_handler_run && (source = NexusOperation_0, target = WorkflowRun_1) in relation_nexus_operation_handler_run), "relation nexus-operation-handler-run exceeds source cardinality";
    assert !((source = NexusOperation_1, target = WorkflowRun_0) in relation_nexus_operation_handler_run && (source = NexusOperation_1, target = WorkflowRun_1) in relation_nexus_operation_handler_run), "relation nexus-operation-handler-run exceeds source cardinality";
    assert ((!(WorkflowRun_0 in exists_WorkflowRun) || ((!((state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_completed || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_failed || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_canceled || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_terminated || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_timed_out || state_WorkflowRun[WorkflowRun_0] == WorkflowRun_state_continued_as_new)) || (((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged)))))))))))))) && (!(WorkflowRun_1 in exists_WorkflowRun) || ((!((state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_completed || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_failed || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_canceled || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_terminated || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_timed_out || state_WorkflowRun[WorkflowRun_1] == WorkflowRun_state_continued_as_new)) || (((!(Callback_0 in exists_Callback) || ((!((source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_0, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))) && (!(Callback_1 in exists_Callback) || ((!((source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run) || (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_0) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged)))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!((source = Callback_1, target = CallbackDelivery_1) in relation_callback_delivery) || (state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged))))))))))))))), "property CallbackHandlerLifetime failed";
    assert ((!(Callback_0 in exists_Callback) || (((!(NexusOperation_0 in exists_NexusOperation) || (((!(WorkflowRun_0 in exists_WorkflowRun) || ((!(((source = Callback_0, target = NexusOperation_0) in relation_callback_operation && (source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run)) || ((source = NexusOperation_0, target = WorkflowRun_0) in relation_nexus_operation_handler_run)))) && (!(WorkflowRun_1 in exists_WorkflowRun) || ((!(((source = Callback_0, target = NexusOperation_0) in relation_callback_operation && (source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run)) || ((source = NexusOperation_0, target = WorkflowRun_1) in relation_nexus_operation_handler_run))))))) && (!(NexusOperation_1 in exists_NexusOperation) || (((!(WorkflowRun_0 in exists_WorkflowRun) || ((!(((source = Callback_0, target = NexusOperation_1) in relation_callback_operation && (source = Callback_0, target = WorkflowRun_0) in relation_callback_handler_run)) || ((source = NexusOperation_1, target = WorkflowRun_0) in relation_nexus_operation_handler_run)))) && (!(WorkflowRun_1 in exists_WorkflowRun) || ((!(((source = Callback_0, target = NexusOperation_1) in relation_callback_operation && (source = Callback_0, target = WorkflowRun_1) in relation_callback_handler_run)) || ((source = NexusOperation_1, target = WorkflowRun_1) in relation_nexus_operation_handler_run)))))))))) && (!(Callback_1 in exists_Callback) || (((!(NexusOperation_0 in exists_NexusOperation) || (((!(WorkflowRun_0 in exists_WorkflowRun) || ((!(((source = Callback_1, target = NexusOperation_0) in relation_callback_operation && (source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run)) || ((source = NexusOperation_0, target = WorkflowRun_0) in relation_nexus_operation_handler_run)))) && (!(WorkflowRun_1 in exists_WorkflowRun) || ((!(((source = Callback_1, target = NexusOperation_0) in relation_callback_operation && (source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run)) || ((source = NexusOperation_0, target = WorkflowRun_1) in relation_nexus_operation_handler_run))))))) && (!(NexusOperation_1 in exists_NexusOperation) || (((!(WorkflowRun_0 in exists_WorkflowRun) || ((!(((source = Callback_1, target = NexusOperation_1) in relation_callback_operation && (source = Callback_1, target = WorkflowRun_0) in relation_callback_handler_run)) || ((source = NexusOperation_1, target = WorkflowRun_0) in relation_nexus_operation_handler_run)))) && (!(WorkflowRun_1 in exists_WorkflowRun) || ((!(((source = Callback_1, target = NexusOperation_1) in relation_callback_operation && (source = Callback_1, target = WorkflowRun_1) in relation_callback_handler_run)) || ((source = NexusOperation_1, target = WorkflowRun_1) in relation_nexus_operation_handler_run))))))))))), "property CallbackReferenceConsistency failed";
    assert (((!(CallbackDelivery_0 in exists_CallbackDelivery) || ((!(state_CallbackDelivery[CallbackDelivery_0] == CallbackDelivery_state_acknowledged) || (((CallbackResponse_0 in exists_CallbackResponse && (((source = CallbackDelivery_0, target = CallbackResponse_0) in relation_callback_delivery_response && state_CallbackResponse[CallbackResponse_0] == CallbackResponse_state_accepted))) || (CallbackResponse_1 in exists_CallbackResponse && (((source = CallbackDelivery_0, target = CallbackResponse_1) in relation_callback_delivery_response && state_CallbackResponse[CallbackResponse_1] == CallbackResponse_state_accepted)))))))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || ((!(state_CallbackDelivery[CallbackDelivery_1] == CallbackDelivery_state_acknowledged) || (((CallbackResponse_0 in exists_CallbackResponse && (((source = CallbackDelivery_1, target = CallbackResponse_0) in relation_callback_delivery_response && state_CallbackResponse[CallbackResponse_0] == CallbackResponse_state_accepted))) || (CallbackResponse_1 in exists_CallbackResponse && (((source = CallbackDelivery_1, target = CallbackResponse_1) in relation_callback_delivery_response && state_CallbackResponse[CallbackResponse_1] == CallbackResponse_state_accepted))))))))) && ((!(CallbackDelivery_0 in exists_CallbackDelivery) || (((!(CallbackResponse_0 in exists_CallbackResponse) || ((!((source = CallbackDelivery_0, target = CallbackResponse_0) in relation_callback_delivery_response) || (state_CallbackResponse[CallbackResponse_0] == CallbackResponse_state_accepted)))) && (!(CallbackResponse_1 in exists_CallbackResponse) || ((!((source = CallbackDelivery_0, target = CallbackResponse_1) in relation_callback_delivery_response) || (state_CallbackResponse[CallbackResponse_1] == CallbackResponse_state_accepted))))))) && (!(CallbackDelivery_1 in exists_CallbackDelivery) || (((!(CallbackResponse_0 in exists_CallbackResponse) || ((!((source = CallbackDelivery_1, target = CallbackResponse_0) in relation_callback_delivery_response) || (state_CallbackResponse[CallbackResponse_0] == CallbackResponse_state_accepted)))) && (!(CallbackResponse_1 in exists_CallbackResponse) || ((!((source = CallbackDelivery_1, target = CallbackResponse_1) in relation_callback_delivery_response) || (state_CallbackResponse[CallbackResponse_1] == CallbackResponse_state_accepted))))))))), "property CallbackResponseConsistency failed";
  }

  fun CheckQuiescent() {
  }

}

test tcUmpire [main=UmpireWorld]: { UmpireWorld };
