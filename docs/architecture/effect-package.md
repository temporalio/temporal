# `effect` package

## Motivation
When processing an operation, Temporal's persistence accumulates changes to the mutable state and only
commits them to the database at the end of the operation. For a Workflow Update, the caller of the
`UpdateWorkflowExecution` API can only be unblocked after the database write has successfully completed, 
ensuring that the update is durable.

Unblocking the caller immediately after processing the worker's Update result in 
`RespondWorkflowTaskCompleted` would introduce a poor design by tightly coupling these two separate concerns.

To address this, the effect package was introduced.

## How it works
`effect.Buffer` allows to accumulate callbacks - in the same way persistence accumulates changes
- and exposes methods to `Apply()` or `Cancel()` them.

For each operation, two callbacks should be registered: one for the success case, and one for when 
the operation needs to be canceled. Then, the `Apply()` method should be called right after the
operation succeeded - or `Cancel()` when it failed (usually in the `defer` block). The order the callbacks
are executed in is the same order they were registered when using `Apply()`, and reversed for `Cancel()`.

#### NOTE
> It is important to note that the `effect` package *does not* provide any transactional guarantees.
> For example, the callbacks in `effect.Buffer` can be partially applied (or not applied at all)
> after the persistence write completed successfully (which is acceptable for the Workflow Update case).  

## Usage
This package was created specifically for the Workflow Update feature, and is currently used only there.
It is not coupled with Workflow Update, though, and can be used anywhere where delayed execution is required. 

> #### TODO
> `workflowTaskResponseMutation` is one of the candidates to migrate to `effect` package.
