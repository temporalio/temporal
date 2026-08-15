---- MODULE Umpire ----
EXTENDS FiniteSets, Naturals

CONSTANTS
    \* @type: Set(Str);
    CallbackIDs,
    \* @type: Set(Str);
    CallbackDeliveryIDs,
    \* @type: Set(Str);
    CallbackResponseIDs,
    \* @type: Set(Str);
    WorkflowRunIDs

VARIABLES
    \* @type: Set(Str);
    exists_Callback,
    \* @type: Str -> Str;
    state_Callback,
    \* @type: Set(Str);
    exists_CallbackDelivery,
    \* @type: Str -> Str;
    state_CallbackDelivery,
    \* @type: Set(Str);
    exists_CallbackResponse,
    \* @type: Str -> Str;
    state_CallbackResponse,
    \* @type: Set(Str);
    exists_WorkflowRun,
    \* @type: Str -> Str;
    state_WorkflowRun,
    \* @type: Set(<<Str, Str>>);
    relation_callback_delivery,
    \* @type: Set(<<Str, Str>>);
    relation_callback_delivery_response,
    \* @type: Set(<<Str, Str>>);
    relation_callback_handler_run

vars == <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

TypeOK ==
    /\ exists_Callback \in SUBSET CallbackIDs
    /\ state_Callback \in [CallbackIDs -> {"unobserved"}]
    /\ exists_CallbackDelivery \in SUBSET CallbackDeliveryIDs
    /\ state_CallbackDelivery \in [CallbackDeliveryIDs -> {"acknowledged", "delivered", "failed", "pending"}]
    /\ exists_CallbackResponse \in SUBSET CallbackResponseIDs
    /\ state_CallbackResponse \in [CallbackResponseIDs -> {"accepted", "conflicting", "unobserved"}]
    /\ exists_WorkflowRun \in SUBSET WorkflowRunIDs
    /\ state_WorkflowRun \in [WorkflowRunIDs -> {"canceled", "completed", "continued_as_new", "created", "failed", "started", "terminated", "timed_out"}]
    /\ relation_callback_delivery \in SUBSET (CallbackIDs \X CallbackDeliveryIDs)
    /\ relation_callback_delivery_response \in SUBSET (CallbackDeliveryIDs \X CallbackResponseIDs)
    /\ relation_callback_handler_run \in SUBSET (CallbackIDs \X WorkflowRunIDs)

Cardinality_callback_delivery ==
    /\ \A tuple \in relation_callback_delivery: tuple[1] \in exists_Callback /\ tuple[2] \in exists_CallbackDelivery
    /\ \A target \in CallbackDeliveryIDs: Cardinality({source \in CallbackIDs: <<source, target>> \in relation_callback_delivery}) <= 1

Cardinality_callback_delivery_response ==
    /\ \A tuple \in relation_callback_delivery_response: tuple[1] \in exists_CallbackDelivery /\ tuple[2] \in exists_CallbackResponse
    /\ \A source \in CallbackDeliveryIDs: Cardinality({target \in CallbackResponseIDs: <<source, target>> \in relation_callback_delivery_response}) <= 1
    /\ \A target \in CallbackResponseIDs: Cardinality({source \in CallbackDeliveryIDs: <<source, target>> \in relation_callback_delivery_response}) <= 1

Cardinality_callback_handler_run ==
    /\ \A tuple \in relation_callback_handler_run: tuple[1] \in exists_Callback /\ tuple[2] \in exists_WorkflowRun
    /\ \A source \in CallbackIDs: Cardinality({target \in WorkflowRunIDs: <<source, target>> \in relation_callback_handler_run}) <= 1

Init ==
    /\ exists_Callback = {}
    /\ state_Callback = [entity \in CallbackIDs |-> "unobserved"]
    /\ exists_CallbackDelivery = {}
    /\ state_CallbackDelivery = [entity \in CallbackDeliveryIDs |-> "pending"]
    /\ exists_CallbackResponse = {}
    /\ state_CallbackResponse = [entity \in CallbackResponseIDs |-> "unobserved"]
    /\ exists_WorkflowRun = {}
    /\ state_WorkflowRun = [entity \in WorkflowRunIDs |-> "created"]
    /\ relation_callback_delivery = {}
    /\ relation_callback_delivery_response = {}
    /\ relation_callback_handler_run = {}

Callback_attach_handlerEnabled(callback, handlerRun) ==
    /\ callback \in CallbackIDs
    /\ callback \notin exists_Callback
    /\ handlerRun \in WorkflowRunIDs
    /\ handlerRun \in exists_WorkflowRun
    /\ state_WorkflowRun[handlerRun] = "started"

Callback_attach_handler(callback, handlerRun) ==
    /\ Callback_attach_handlerEnabled(callback, handlerRun)
    /\ exists_Callback' = exists_Callback \union {callback}
    /\ state_Callback' = [state_Callback EXCEPT ![callback] = "unobserved"]
    /\ relation_callback_handler_run' = (relation_callback_handler_run) \union {<<callback, handlerRun>>}
    /\ UNCHANGED <<exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response>>

Callback_delivery_acknowledgeEnabled(delivery, response) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ response \in CallbackResponseIDs
    /\ response \notin exists_CallbackResponse
    /\ state_CallbackDelivery[delivery] = "delivered"

Callback_delivery_acknowledge(delivery, response) ==
    /\ Callback_delivery_acknowledgeEnabled(delivery, response)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "acknowledged"]
    /\ exists_CallbackResponse' = exists_CallbackResponse \union {response}
    /\ state_CallbackResponse' = [state_CallbackResponse EXCEPT ![response] = "accepted"]
    /\ relation_callback_delivery_response' = (relation_callback_delivery_response) \union {<<delivery, response>>}
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_handler_run>>

Callback_delivery_deliverEnabled(delivery) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ state_CallbackDelivery[delivery] = "pending"

Callback_delivery_deliver(delivery) ==
    /\ Callback_delivery_deliverEnabled(delivery)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "delivered"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_delivery_enqueueEnabled(callback, delivery) ==
    /\ callback \in CallbackIDs
    /\ callback \in exists_Callback
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \notin exists_CallbackDelivery
    /\ (\A handlerRun \in WorkflowRunIDs: handlerRun \in exists_WorkflowRun => ((<<callback, handlerRun>> \in relation_callback_handler_run => ~((state_WorkflowRun[handlerRun] = "completed" \/ state_WorkflowRun[handlerRun] = "failed" \/ state_WorkflowRun[handlerRun] = "canceled" \/ state_WorkflowRun[handlerRun] = "terminated" \/ state_WorkflowRun[handlerRun] = "timed_out" \/ state_WorkflowRun[handlerRun] = "continued_as_new")))))

Callback_delivery_enqueue(callback, delivery) ==
    /\ Callback_delivery_enqueueEnabled(callback, delivery)
    /\ exists_CallbackDelivery' = exists_CallbackDelivery \union {delivery}
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "pending"]
    /\ relation_callback_delivery' = (relation_callback_delivery) \union {<<callback, delivery>>}
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_delivery_fail_deliveredEnabled(delivery) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ state_CallbackDelivery[delivery] = "delivered"

Callback_delivery_fail_delivered(delivery) ==
    /\ Callback_delivery_fail_deliveredEnabled(delivery)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_delivery_fail_pendingEnabled(delivery) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ state_CallbackDelivery[delivery] = "pending"

Callback_delivery_fail_pending(delivery) ==
    /\ Callback_delivery_fail_pendingEnabled(delivery)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_delivery_retryEnabled(delivery) ==
    /\ delivery \in CallbackDeliveryIDs
    /\ delivery \in exists_CallbackDelivery
    /\ state_CallbackDelivery[delivery] = "failed"

Callback_delivery_retry(delivery) ==
    /\ Callback_delivery_retryEnabled(delivery)
    /\ state_CallbackDelivery' = [state_CallbackDelivery EXCEPT ![delivery] = "pending"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, state_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_handler_close_cancelEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ (\A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => (\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))))

Callback_handler_close_cancel(entity) ==
    /\ Callback_handler_close_cancelEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "canceled"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_handler_close_completeEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ (\A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => (\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))))

Callback_handler_close_complete(entity) ==
    /\ Callback_handler_close_completeEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "completed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_handler_close_continue_as_newEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ (\A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => (\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))))

Callback_handler_close_continue_as_new(entity) ==
    /\ Callback_handler_close_continue_as_newEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "continued_as_new"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_handler_close_failEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ (\A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => (\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))))

Callback_handler_close_fail(entity) ==
    /\ Callback_handler_close_failEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "failed"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_handler_close_terminateEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ (\A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => (\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))))

Callback_handler_close_terminate(entity) ==
    /\ Callback_handler_close_terminateEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "terminated"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_handler_close_timeoutEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \in exists_WorkflowRun
    /\ (state_WorkflowRun[entity] = "started" /\ (\A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, entity>> \in relation_callback_handler_run => (\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))))

Callback_handler_close_timeout(entity) ==
    /\ Callback_handler_close_timeoutEnabled(entity)
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "timed_out"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, exists_WorkflowRun, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Callback_handler_startEnabled(entity) ==
    /\ entity \in WorkflowRunIDs
    /\ entity \notin exists_WorkflowRun
    /\ state_WorkflowRun[entity] = "created"

Callback_handler_start(entity) ==
    /\ Callback_handler_startEnabled(entity)
    /\ exists_WorkflowRun' = exists_WorkflowRun \union {entity}
    /\ state_WorkflowRun' = [state_WorkflowRun EXCEPT ![entity] = "started"]
    /\ UNCHANGED <<exists_Callback, state_Callback, exists_CallbackDelivery, state_CallbackDelivery, exists_CallbackResponse, state_CallbackResponse, relation_callback_delivery, relation_callback_delivery_response, relation_callback_handler_run>>

Next ==
    \/ \E callback \in CallbackIDs, handlerRun \in WorkflowRunIDs: Callback_attach_handler(callback, handlerRun)
    \/ \E delivery \in CallbackDeliveryIDs, response \in CallbackResponseIDs: Callback_delivery_acknowledge(delivery, response)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_deliver(delivery)
    \/ \E callback \in CallbackIDs, delivery \in CallbackDeliveryIDs: Callback_delivery_enqueue(callback, delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_fail_delivered(delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_fail_pending(delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_retry(delivery)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_cancel(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_complete(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_continue_as_new(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_fail(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_terminate(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_timeout(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_start(entity)

CanStep ==
    \/ \E callback \in CallbackIDs, handlerRun \in WorkflowRunIDs: Callback_attach_handlerEnabled(callback, handlerRun)
    \/ \E delivery \in CallbackDeliveryIDs, response \in CallbackResponseIDs: Callback_delivery_acknowledgeEnabled(delivery, response)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_deliverEnabled(delivery)
    \/ \E callback \in CallbackIDs, delivery \in CallbackDeliveryIDs: Callback_delivery_enqueueEnabled(callback, delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_fail_deliveredEnabled(delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_fail_pendingEnabled(delivery)
    \/ \E delivery \in CallbackDeliveryIDs: Callback_delivery_retryEnabled(delivery)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_cancelEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_completeEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_continue_as_newEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_failEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_terminateEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_close_timeoutEnabled(entity)
    \/ \E entity \in WorkflowRunIDs: Callback_handler_startEnabled(entity)

CallbackHandlerLifetime ==
    (\A handlerRun \in WorkflowRunIDs: handlerRun \in exists_WorkflowRun => (((state_WorkflowRun[handlerRun] = "completed" \/ state_WorkflowRun[handlerRun] = "failed" \/ state_WorkflowRun[handlerRun] = "canceled" \/ state_WorkflowRun[handlerRun] = "terminated" \/ state_WorkflowRun[handlerRun] = "timed_out" \/ state_WorkflowRun[handlerRun] = "continued_as_new") => (\A callback \in CallbackIDs: callback \in exists_Callback => ((<<callback, handlerRun>> \in relation_callback_handler_run => (\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((<<callback, delivery>> \in relation_callback_delivery => state_CallbackDelivery[delivery] = "acknowledged")))))))))

CallbackResponseConsistency ==
    ((\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((state_CallbackDelivery[delivery] = "acknowledged" => (\E response \in CallbackResponseIDs: response \in exists_CallbackResponse /\ ((<<delivery, response>> \in relation_callback_delivery_response /\ state_CallbackResponse[response] = "accepted")))))) /\ (\A delivery \in CallbackDeliveryIDs: delivery \in exists_CallbackDelivery => ((\A response \in CallbackResponseIDs: response \in exists_CallbackResponse => ((<<delivery, response>> \in relation_callback_delivery_response => (state_CallbackDelivery[delivery] = "acknowledged" /\ state_CallbackResponse[response] = "accepted")))))))

InductiveInvariant ==
    /\ TypeOK
    /\ Cardinality_callback_delivery
    /\ Cardinality_callback_delivery_response
    /\ Cardinality_callback_handler_run
    /\ CallbackHandlerLifetime
    /\ CallbackResponseConsistency
DeclaredSafety ==
    /\ CallbackHandlerLifetime
    /\ CallbackResponseConsistency
Safety == InductiveInvariant /\ DeclaredSafety

QuiescentSafety == TRUE

Spec == Init /\ [][Next]_vars

====
