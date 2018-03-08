## What
Cadence CLI provide an command-line tool for users to perform various tasks on Cadence.  
Users can perform operations like register, update and describe on domain;  
also start workflow, show workflow history, signal workflow ... and many other tasks.    

## How
- Run `make bins`
- You should see an executable `cadence`

## Quick Start
Run `./cadence` to view help message. There are some top level commands and global options.   
Run `./cadence domain` to view help message about operations on domain  
Run `./cadence workflow` to view help message about operations on workflow  
Run `./cadence tasklist` to view help message about operations on tasklist  
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

**Tips:**  
to avoid repeated input global option **domain**, user can export domain-name in environment variable CADENCE_CLI_DOMAIN.
```
export CADENCE_CLI_DOMAIN=samples-domain

# then just run commands without --domain flag, like
./cadence domain desc
```

### Workflow operation examples
(The following examples assume you already export CADENCE_CLI_DOMAIN environment variable as Tips above)

- Run workflow: Start a workflow and see it's progress, this command doesn't finish until workflow completes
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
and it takes a string as input, so there is the `-i '"cadence"'`. Single quote `''` is used to wrap input as json. 

**Note:** you need to start worker so that workflow can make progress.  
(Run `make && ./bin/helloworld -m worker` in cadence-samples to start the worker)

- Show running workers of a tasklist
```
./cadence tasklist desc --tl helloWorldGroup

```

- Start workflow: 
```
./cadence workflow start --tl helloWorldGroup --wt main.Workflow --et 60 -i '"cadence"'

# view help messages for workflow start
./cadence workflow start -h

# for workflow with multiple input, seperate each json with space/newline like
./cadence workflow start --tl helloWorldGroup --wt main.WorkflowWith3Args --et 60 -i '"your_input_string" 123 {"Name":"my-string", "Age":12345}'
```
Workflow `start` command is similar to `run` command and takes same flag options. But it just start the workflow and immediately return workflow_id and run_id.  
User need to run `show` to view workflow history/progress.  

- Show workflow history
```
./cadence workflow show -w 3ea6b242-b23c-4279-bb13-f215661b4717 -r 866ae14c-88cf-4f1e-980f-571e031d71b0
# a shortcut of this is (without -w -r flag)
./cadence workflow showid 3ea6b242-b23c-4279-bb13-f215661b4717 866ae14c-88cf-4f1e-980f-571e031d71b0

# if run_id is not provided, it will show the latest run history for that workflow_id
./cadence workflow show -w 3ea6b242-b23c-4279-bb13-f215661b4717
# a shortcut of this is
./cadence workflow showid 3ea6b242-b23c-4279-bb13-f215661b4717
```

- List open or closed workflow executions
```
./cadence workflow list
```

- Query workflow execution
```
# use custom query type
./cadence workflow query -w <wid> -r <rid> --qt <query-type>

# use build-in query type "__stack_trace" which is supported by cadence client library
./cadence workflow query -w <wid> -r <rid> --qt __stack_trace
# a shortcut to query using __stack_trace is (without --qt flag)
./cadence workflow stack -w <wid> -r <rid> 
```

- Signal, cancel, terminate workflow
```
# signal
./cadence workflow signal -w <wid> -r <rid> -n <signal-name> -i '"signal-value"'

# cancel
./cadence workflow cancel -w <wid> -r <rid>

# terminate
./cadence workflow terminate -w <wid> -r <rid> --reason 
```
Terminate a running workflow execution will record WorkflowExecutionTerminated event as closing event in the history. No more decision task will be scheduled for terminated workflow execution.  
Cancel a running workflow execution will record WorkflowExecutionCancelRequested event in the history, and a new decision task will be scheduled. Workflow has a chance to do some clean up work after cancellation.