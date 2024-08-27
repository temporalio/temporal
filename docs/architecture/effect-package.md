# `effect` package

## Motivation
Acceptance or completion of Workflow Update involves 2 steps:
1. Corresponding history events need to be created together with mutable state updates (persistence operation),
2. Caller of `UpdateWorkflowExecution` API should get be unblocked with Update results.

In Temporal persistence updates are accumulated during execution of operation/handler and actual
write is performed at the very end of the operation. The write operation can fail which means
that caller of `UpdateWorkflowExecution` API can't be unblocked until write succeeded. The naive
approach would be to split 2 steps above, and, first, create events and update mutable state, and, then,
after successfully write unblock caller. This is not great because the code of these 2 steps is highly
coupled, and it is better to have it in one place. Also adding more condition and branched
to already complicated `RespondWorkflowTaskCompleted` API handler must be avoided. 

Instead, `effect` package was introduced. `effect.Buffer` struct collects a set of callbacks
(same way as mutable state collects persistence updates), and expose method to `Apply()` or `Cancel()`
them.

For each operation, two callbacks should be registered: one for a success case, and one in case if
operation needs to be canceled. Then `Apply()` method should be called right after first operation
succeeds and `Cancel()` if it failed (usually in `deffer` block). Order of execution is the same
as register order for `Apply()`, and reversed for `Cancel()`.

> #### NOTE
> It is important to point out that `effect` package doesn't provide any transaction guarantees.
> For example `effect.Buffer` can be partially applied (or not applied at all) after persistence
> operation completed successfully (which is acceptable for a Workflow Update case).  

## Usage
This package was created for Workflow Update feature, and currently used only there. It is not coupled
with Workflow Update though, and can be used anywhere where delayed execution is required. 

> #### TODO
> `workflowTaskResponseMutation` is one of the candidates to migrate to `effect` package.
