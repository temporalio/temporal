## What
Cadence CLI is a command-line tool to perform various tasks on a Cadence server.
It can perform domain operations such as register, update, and describe as well as workflow operations like
start workflow, show workflow history, and signal workflow.

## How
- Run `make bins`
- You should see an executable `cadence`
- (Optional) You could also use docker image `ubercadence/cli`, by replacing all the following `./cadence ...` with `docker run --rm ubercadence/cli:master ...`

## Quick Start
Run `./cadence` for help on top level commands and global options   
Run `./cadence domain` for help on domain operations  
Run `./cadence workflow` for help on workflow operations  
Run `./cadence tasklist` for help on tasklist operations  
(`./cadence help`, `./cadence help [domain|workflow]` will also print help messages)

**Note:** make sure you have cadence server running before using CLI 

### Domain operation examples 
- Register a new domain named "samples-domain":  
```
./cadence --domain samples-domain domain register 
# OR using short alias  
./cadence --do samples-domain domain re
```   
- View "samples-domain" details:   
```
./cadence --domain samples-domain domain describe  
```

**Tip:**  
To avoid repeatedly including the global option **--domain**, 
export domain-name to the CADENCE_CLI_DOMAIN environment variable.
```
export CADENCE_CLI_DOMAIN=samples-domain
```
Then commands can omit the **--domain** flag:
```
./cadence domain desc
```

### Workflow operation examples
(The following examples assume the CADENCE_CLI_DOMAIN environment variable is set using the tip above)

#### Run workflow: Start a workflow and see it's progress. This command doesn't finish until workflow completes.
```
./cadence workflow run --tl helloWorldGroup --wt main.Workflow --et 60 -i '"cadence"'

# view help messages for workflow run
./cadence workflow run -h
``` 
Brief explanation:  
To run a workflow, user must specify 
1. Tasklist name (--tl), 
2. Workflow type (--wt), 
3. Execution start to close timeout in seconds (--et), 

Example uses [this cadence-samples workflow](https://github.com/samarabbas/cadence-samples/blob/master/cmd/samples/recipes/helloworld/helloworld_workflow.go) 
and takes a string as input with the `-i '"cadence"'` parameter. Single quotes (`''`) are used to wrap input as json. 

**Note:** you need to start the worker so that workflow can make progress.  
(Run `make && ./bin/helloworld -m worker` in cadence-samples to start the worker)

#### Show running workers of a tasklist
```
./cadence tasklist desc --tl helloWorldGroup
```

#### Start workflow
```
./cadence workflow start --tl helloWorldGroup --wt main.Workflow --et 60 -i '"cadence"'

# view help messages for workflow start
./cadence workflow start -h

# for workflow with multiple input, separate each json with space/newline like
./cadence workflow start --tl helloWorldGroup --wt main.WorkflowWith3Args --et 60 -i '"your_input_string" 123 {"Name":"my-string", "Age":12345}'
```
The workflow `start` command is similar to the `run` command, but immediately returns the workflow_id and 
run_id after starting the workflow. Use the `show` command to view the workflow's history/progress.  

**Re-use the same workflow id when starting/running workflow**

Use option `--workflowidreusepolicy` or `--wrp` to configure the workflow id re-use policy.  
**Option 0 AllowDuplicateFailedOnly:** Allow starting a workflow execution using the same workflow ID when a workflow with the same workflow ID is not already running and the last execution close state is one of *[terminated, cancelled, timedout, failed]*.  
**Option 1 AllowDuplicate:** Allow starting a workflow execution using the same workflow ID when a workflow with the same workflow ID is not already running.  
**Option 2 RejectDuplicate:** Do not allow starting a workflow execution using the same workflow ID as a previous workflow.  
```
# use AllowDuplicateFailedOnly option to start a workflow
./cadence workflow start --tl helloWorldGroup --wt main.Workflow --et 60 -i '"cadence"' --wid "<duplicated workflow id>" --wrp 0

# use AllowDuplicate option to run a workflow
./cadence workflow run --tl helloWorldGroup --wt main.Workflow --et 60 -i '"cadence"' --wid "<duplicated workflow id>" --wrp 1
```

#### Show workflow history
```
./cadence workflow show -w 3ea6b242-b23c-4279-bb13-f215661b4717 -r 866ae14c-88cf-4f1e-980f-571e031d71b0
# a shortcut of this is (without -w -r flag)
./cadence workflow showid 3ea6b242-b23c-4279-bb13-f215661b4717 866ae14c-88cf-4f1e-980f-571e031d71b0

# if run_id is not provided, it will show the latest run history of that workflow_id
./cadence workflow show -w 3ea6b242-b23c-4279-bb13-f215661b4717
# a shortcut of this is
./cadence workflow showid 3ea6b242-b23c-4279-bb13-f215661b4717
```

#### Show workflow execution info
```
./cadence workflow describe -w 3ea6b242-b23c-4279-bb13-f215661b4717 -r 866ae14c-88cf-4f1e-980f-571e031d71b0
# a shortcut of this is (without -w -r flag)
./cadence workflow describeid 3ea6b242-b23c-4279-bb13-f215661b4717 866ae14c-88cf-4f1e-980f-571e031d71b0

# if run_id is not provided, it will show the latest workflow execution of that workflow_id
./cadence workflow describe -w 3ea6b242-b23c-4279-bb13-f215661b4717
# a shortcut of this is
./cadence workflow describeid 3ea6b242-b23c-4279-bb13-f215661b4717
```

#### List closed or open workflow executions
```
./cadence workflow list

# default will only show one page, to view more items, use --more flag
./cadence workflow list -m
```

#### Query workflow execution
```
# use custom query type
./cadence workflow query -w <wid> -r <rid> --qt <query-type>

# use build-in query type "__stack_trace" which is supported by cadence client library
./cadence workflow query -w <wid> -r <rid> --qt __stack_trace
# a shortcut to query using __stack_trace is (without --qt flag)
./cadence workflow stack -w <wid> -r <rid> 
```

#### Signal, cancel, terminate workflow
```
# signal
./cadence workflow signal -w <wid> -r <rid> -n <signal-name> -i '"signal-value"'

# cancel
./cadence workflow cancel -w <wid> -r <rid>

# terminate
./cadence workflow terminate -w <wid> -r <rid> --reason 
```
Terminating a running workflow execution will record a WorkflowExecutionTerminated event as the closing event in the history. No more decision tasks will be scheduled for a terminated workflow execution.  
Canceling a running workflow execution will record a WorkflowExecutionCancelRequested event in the history, and a new decision task will be scheduled. The workflow has a chance to do some clean up work after cancellation.

#### Restart, reset workflow
The Reset command allows resetting a workflow to a particular point and continue running from there.
There are a lot of use cases:
- Rerun a failed workflow from the beginning with the same start parameters.
- Rerun a failed workflow from the failing point without losing the achieved progress(history).
- After deploying new code, reset an open workflow to let the workflow run to different flows.

```
./cadence workflow reset -w <wid> -r <rid> --event_id <decision_finish_event_id> --reason "some_reason"
```
Some things to note:
- When reset, a new run will be kicked off with the same workflowID. But if there is a running execution for the workflow(workflowID), the current run will be terminated.
- decision_finish_event_id is the ID of events of the type: DecisionTaskComplete/DecisionTaskFailed/DecisionTaskTimeout.
- To restart a workflow from the beginning, reset to the first decision task finish event.