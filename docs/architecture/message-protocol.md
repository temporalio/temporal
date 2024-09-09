# Message protocol

## Why it exists
Usually communication between server and worker uses events and commands: events go from server to worker,
worker process them and generates commands that go back to server. Events are attached to Workflow Task, which
worker gets as response to `PollWorkflowTask` API call, and worker sends commands back
when it completes Workflow Task with `RespondWorkflowTaskCompleted` API. Workflow Task works as transport on RPC level.

Unfortunately, this way or communication didn't work for Workflow Update. Server can't use events
to ship Update request to the worker, because worker might reject Update, and it must completely disappear.
Because history is immutable, server can't delete events from it. Initial implementation
was using transient event with Update request, which wasn't written to history. This implementation
was proven to be error-prone and hard to handle on the SDK side. Commands that go back from worker to server
also can't be used for Update because some SDKs assume that every command will produce exactly one event, 
which is not true for Update rejections that don't produce any events.

Another protocol was required to implement Workflow Update. Messages are attached to Workflow Task and go in
both directions, similar to events and commands but don't have limitations listed above.

## `Message` proto message
This might look confusing:
```protobuf
message Message {}
```
but first `message` word refers to protobuf messages and second `Message` is `protocolpb.Message`
data struct used by Temporal. Most fields are self-explanatory, but some fields need explanation.

### `protocol_instance_id`
is an identifier of the object which this message belongs to. Because currently messages are used for
Workflow Update only, it is `update_id`.

> #### TODO
> In the future, signals and queries might use message protocol too.
> In these case `protocol_instance_id` will be `query_id` or `signal_id.

### `body`
is intentionally of type `Any` (not `oneof`) to support pluggable interfaces which Temporal server
might not be aware of.

### `sequence_id`
Because messages might intersect with events and commands, it is important to specify when
a particular message must be processed. This field can be `event_id`, and it will indicate event
after which message should be processed by worker, or `command_index`, and it will indicate
command after which message should be processed by server.

> #### TODO
> In fact, this is not used. Server always set `event_id` equal to event Id before `WorkflowTaskStartedEvent`,
> which essentially means, that all messages are processed after all events (all SDKs respect this field though).
> This is because buffered events are reordered on server (see `reorderBuffer()` func) and intersection 
> with them based on `event_id` is not possible. When reordering is removed, this field can be set to the right value.
> 
> `command_index` is not used because SDKs use different approach: special command of type `COMMAND_TYPE_PROTOCOL_MESSAGE`
> is added to a command list to indicate place where a message must be processed. This command has only `message_id` fields
> which points to a particular message. This is done this way because of limitation described above:
> all commands must produce exactly one event and vice versa. Because Update rejection messages 
> doesn't produce events at all, `COMMAND_TYPE_PROTOCOL_MESSAGE` is not added to the list of commands for Update rejections.
> Once processed, a message is removed from the list of messages to process. Therefore,
> all Update rejections, as well as messages which don't have `COMMAND_TYPE_PROTOCOL_MESSAGE` command
> are processed last (because messages are processed after commands).
> Server doesn't require `COMMAND_TYPE_PROTOCOL_MESSAGE` command, and if it is not present, all messages
> will be processed after all commands in the order they arrive.
> When 1:1 limitation is removed, `command_index` might be used.

> #### NOTE
> All SDKs process all queries last (after events and messages).
