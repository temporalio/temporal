# Workflow lifecycle

Below we follow a typical sequence of events in the execution of the following very simple workflow:

```
myWorkflow() {
   result = callActivity(myActivity)
   return result
}
```

<br>

---

<br>

**1. The User Application uses a Temporal SDK to send a `StartWorkflowExecution` request; a Workflow Task is added in the Matching service**

```mermaid
sequenceDiagram
User Application->>Frontend: StartWorkflowExecution
Frontend->> History: StartWorkflowExecution
History ->> Persistence: CreateWorkflowExecution
Persistence ->> Persistence: Persist MutableState and history tasks
Persistence ->> History: Create Succeed
History->>Frontend: Start Succeed
Frontend->>User Application: Start Succeed
loop QueueProcessor
    History->>Persistence: GetHistoryTasks
		History->>History: ProcessTask
		History->>Matching: AddWorkflowTask
end
```

**Code entrypoints:**

- History service [`StartWorkflow` handler](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/service/history/api/startworkflow/api.go#L157).
- History service [queue processors](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/service/history/history_engine.go#L303) and [transfer task queue processor](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/service/history/queues/queue_immediate.go#L150).

<br>

---

<br>

**2. The Worker dequeues the Workflow Task, advances the workflow execution, and becomes blocked on the Activity call.**

```mermaid
sequenceDiagram
Worker->>Frontend: PollWorkflowTask
Frontend->>Matching: PollWorkflowTask
History->>Matching: AddWorkflowTask
Matching->>History: RecordWorkflowTaskStarted
History->>Persistence: UpdateWorkflowExecution
Persistence->>Persistence: Update MutableState & Add timeout timer
Persistence->>History: Update Succeed
History->>Matching: Record Succeed
Matching->>Frontend: WorkflowTask
Frontend->>Persistence: GetHistoryEvents
Persistence->>Frontend: History Events
Frontend->>Worker: WorkflowTask
loop Replayer
    Worker->>Worker: ProcessEvent
end
```

**Code entrypoints:**

- History service [`RecordWorkflowTaskStarted` handler](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/service/history/handler.go#L319)

<br>

---

<br>

**3. The Worker sends a `ScheduleActivityTask` command; an Activity task is added in the Matching service.**

```mermaid
sequenceDiagram
Worker ->> Frontend: RespondWorkflowTaskCompleted(ScheduleActivityTask)
Frontend->> History: RespondWorkflowTaskCompleted(ScheduleActivityTask)
History ->> Persistence: UpdateWorkflowExecution
Persistence ->> Persistence: Persist MutableState and history tasks
Persistence ->> History: Update Succeed
History->>Frontend: Respond Succeed
Frontend->>Worker: Respond Succeed
loop QueueProcessor
    History->>Persistence: GetHistoryTasks
		History->>History: ProcessTask
		History->>Matching: AddActivityTask
end
```

**Code entrypoints:**

- History service [`ScheduleActivityTask` command handler](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/service/history/workflow_task_handler.go#L338)

<br>

---

<br>

**4. The Worker dequeues the Activity task and executes the activity**

```mermaid
sequenceDiagram
Worker->>Frontend: PollActivityTask
Frontend->>Matching: PollActivityTask
History->>Matching: AddActivityTask
Matching->>History: RecordActivityStarted
History->>Persistence: UpdateWorkflowExecution
Persistence->>Persistence: Update MutableState, add timeout timer
Persistence->>History: Update succeed
History->>Matching: Record Succeed
Matching->>Frontend: ActivityTask
Frontend->>Worker: ActivityTask
Worker->>Worker: Execute activity
```

**Code entrypoints:**

- History service [`RecordActivityTaskStarted` handler](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/service/history/handler.go#L287)

<br>

---

<br>

**4. The Worker sends `RespondActivityCompleted`; a Workflow Task is added in the Matching service**

```mermaid
sequenceDiagram
Worker->>Frontend: RespondActivityCompleted
Frontend->>History: RespondActivityCompleted
History->>Persistence: UpdateWorkflowExecution
Persistence->>Persistence: Update MutableState & add transfer task
Persistence->>History: Update Succeed
History->>Frontend: Respond Succeed
Frontend->>Worker: Respond Succeed
loop QueueProcessor
    History->>Persistence: GetHistoryTasks
		History->>History: ProcessTask
		History->>Matching: AddWorkflowTask
end
```

**Code entrypoints:**

- History service [`RespondActivityTaskCompleted` handler](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/service/history/handler.go#L361)

<br>

---

<br>

**5. The Worker dequeues the Workflow Task, advances the workflow, and finds that it has reached its end**

\<Same sequence diagram as step 2 above\>

<br>

---

<br>

**6. The Worker sends `RespondWorkflowTaskCompleted`**

```mermaid
sequenceDiagram
Worker->>Frontend: RespondWorkflowTaskCompleted
Frontend->>History: RespondWorkflowTaskCompleted
History->>Persistence: UpdateWorkflowExecution
Persistence->>Persistence: Update MutableState & add tasks (visibility, tiered storage, retention etc)
Persistence->>History: Update Succeed
History->>Frontend: Respond Succeed
Frontend->>Worker: Respond Succeed
loop QueueProcessor
    History->>Persistence: GetHistoryTasks
		History->>History: ProcessTask (Update visibility, Upload to S3, Delete data etc)
end
```

**Code entrypoints:**

- History service [`RespondWorkflowTaskCompleted` handler](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/service/history/handler.go#L478)

<br>

---

<br>

**Alternatively, the Activity may fail and be retried:**

```mermaid
sequenceDiagram
Worker->>Frontend: RespondActivityFailed
Frontend->>History: RespondActivityFailed
History->>Persistence: UpdateWorkflowExecution
Persistence->>Persistence: Update MutableState & add retry timer
Persistence->>History: Update Succeed
History->>Frontend: Respond Succeed
Frontend->>Worker: Respond Succeed
loop QueueProcessor
    History->>Persistence: GetHistoryTasks
		History->>History: ProcessTask
		History->>Matching: AddActivityTask
end
```

**Code entrypoints:**

- History service [`RespondActivityTaskFailed` handler](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/service/history/handler.go#L400)
