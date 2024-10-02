# Message Protocol

## Why it exists
Usually, communication between the server and the worker uses events and commands: events go from
the server to the worker, the worker processes them and generates commands that go back to server.
The events are attached to the Workflow Task, which the worker receives from the `PollWorkflowTask`
API call, and the worker sends commands back when it completes the Workflow Task with the 
`RespondWorkflowTaskCompleted` API.

Unfortunately, this protocol didn't work for Workflow Update. The server cannot use events to ship
Update request to the worker because in case the Update is rejected, it must completely disappear.
But because the history is immutable, the server cannot delete any events from it. The initial
implementation was using a transient (not written to history) event instead, but that implementation
proved to be error-prone and hard to handle on the SDK side. Similarly, commands can't be used
for Update either because some SDKs assume that every command will produce *exactly* one event, 
which is not true for Update rejections as they don't produce an event.

Another protocol was required to implement Workflow Update: messages are attached to Workflow Task
and travel in both directions. They are similar to events and commands but don't have the same 
limitations listed above.

## `Message` proto message
```protobuf
message Message {}
```
The first `message` refers to protobuf messages and the second `Message` is `protocolpb.Message`
data struct used by Temporal.

### `protocol_instance_id`
This field identifies what object this message belongs to. Because currently messages are only used for
Workflow Update, it is the same as `update_id`.

> #### TODO
> In the future, signals and queries might use the message protocol, too.
> In that case `protocol_instance_id` would be `query_id` or `signal_id`.

### `body`
This field is intentionally of type `Any` (not `oneof`) to support pluggable interfaces which the 
Temporal server might not be aware of.

### `sequencing_id`
Because messages might intersect with events and commands, it is important to specify in what order
messages must be processed. This field can be:
- `event_id`, to indicate the event after which this message should be processed by the worker, or
- `command_index`, to indicate the command after which message should be processed by the server.

> #### TODO
> `event_id` is not used as intended: it is *always* set to the id before the `WorkflowTaskStartedEvent`
> by the server, which means that all messages are processed after all events. This is because buffered
> events are reordered on the server (see `reorderBuffer()` func) and intersecting them based on `event_id`
> is not possible. When reordering is removed, this field can be set to the right value.

> #### TODO
> `command_index` is not used: SDKs use a different approach where a special command of type
> `COMMAND_TYPE_PROTOCOL_MESSAGE` is added to a command list to indicate the place where a message
> must be processed. This command has only `message_id` fields which point to a particular message.
>
> When the Update is rejected, `COMMAND_TYPE_PROTOCOL_MESSAGE` is *not* added to the list of commands,
> though, because of the aforementioned limitation of requiring each command to produce an event.
> The server will assume that any message that wasn't mentioned in a `COMMAND_TYPE_PROTOCOL_MESSAGE`
> command is rejected. Those messages will be processed after all commands were processed first,
> in the order they arrived in.
> 
> When the 1:1 limitation between commands and events is removed, `command_index` can be used.

> #### NOTE
> All SDKs process all queries *last* (i.e. after events and messages).
