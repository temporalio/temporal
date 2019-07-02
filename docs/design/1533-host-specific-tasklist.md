# Proposal: Resource-Specific Tasklist

Author: Yichao Yang (yycptt)

Last updated: April 2019

Discussion at <https://github.com/uber-go/cadence-client/issues/697>

## Abstract

Currently, there’s no straightforward way for a Cadence user to run multiple activities (a session) on a single activity worker. Although there is the option to explicitly specify a host-specific tasklist name or manually execute special activities to get information about the activity worker, these approaches have limitations and are error prone. Besides running multiple activities on a single worker, a user may also want to limit the total number of sessions running concurrently on a single worker if those sessions consume worker resources. This document proposes several new APIs and their implementations so that a user can create sessions, execute activities, and limit the number of concurrent sessions through a simple API call.

## Use Cases

### Machine Learning Model Training

A user can model the entire training process with multiple activities, for example, downloading datasets, data cleaning, model training, and parameter uploading. In this case, all activities should be executed by one activity worker. Otherwise, the dataset might be downloaded multiple times. In addition, as a dataset typically consumes a lot of disk space and model training requires GPU support, the number of total models training on a single worker should be limited.

### Service Deployment

In this case, a session represents the deployment of a service to one worker and the number of concurrent deployments is critical. A user controls that number by limiting the number of concurrently running sessions on an activity worker.

## Proposal

The scope of this proposal is limited to the Cadence client. Cadence server doesn’t need to be changed at all. The basic idea here is that we should write special workflow code for users to perform activity scheduling and worker monitoring, and expose simple APIs to users. Once users don’t need to care about those implementation details, they can focus on their own business logic.

The proposal includes both API changes and implementations. It aims at solving three problems:

1. How to ensure multiple activities are executed on one activity worker.

2. How to limit the number of concurrent sessions on an activity worker.

3. How to carry over session information between different workflow runs.

The following sections will first go through the APIs exposed to users, explain how to use them, and provide sample code. Any developer with previous Cadence experience should be able to understand the APIs and start using them immediately. After that I will explain the model behind those APIs and how they are implemented.

### API Changes

#### New Go Client API

There will be four new APIs available when writing a workflow:

```go
sessionCtx, err := workflow.**CreateSession**(ctx)
```

A user calls this API to create a session and use the returned sessionCtx to execute activities. All activities executed within the returned sessionCtx are considered to be part of the session and will be executed on the same worker. It’s possible that this call will fail due to various reasons, for example, no worker is available at this time.

The session will be created on one of the workers that polls from the tasklist specified in the context passed into the CreateSession API. As a result, users need to make sure they are using the correct task list.

When executing an activity within a session, a user may get two types of errors. The first one is returned from user activities. The session will not be marked as failed in this case, so the user can return whatever error they want and apply their business logic as necessary. If a user wants to end a session due to the error returned from the activity, use the CompleteSession() API below. The second type of error is a special error which says the session has failed due to worker failure and the session is marked as failed in the background. In this case, no activities can be executed using this context. The user can choose how to handle the failure. They can create a new session to retry or end the workflow with an error.

```go
err := workflow.**CompleteSession**(sessionCtx)
```

This API is used to complete a session. It will return an error if the context passed in does not contain a session with state "Open" or it fails to complete the session.

```go
sessionInfo, err := workflow.**GetSessionInfo**(sessionCtx)
```

This API returns session information stored in the context. It returns an error if the context passed in doesn’t contain any session information or the session has already failed. For now, the only information returned is sessionID, which is a unique identifier for a session.

```go
sessionCtx, err := workflow.**RecreateSession**(ctx, sessionInfo)
```

This API takes session information and recreates a session. All the activities in the recreated session will be executed by the specified worker. The primary use case will be continueAsNew. Before returning the continueAsNew error, the user should get the sessionInfo from sessionCtx and complete the session. Then in the next run, the session can be recreated and activities in this new run can continue to be executed on the same activity worker as previous runs.

#### Example

The basic usage looks like the following (it belongs to a larger workflow):

```go
sessionCtx, err := CreateSession(ctx)

if err != nil {

    // Creation failed. Wrong ctx or too many outstanding sessions.

}

err = ExecuteActivity(sessionCtx, DownloadFileActivity, filename).Get(sessionCtx, nil)

if err != nil {

    // Session(worker) has failed or activity itself returns an error.

    // User can perform normal error handling here and decide whether creating a new session is needed.

    // If they decide to create a new session, they need to call CompleteSession() so that worker resources can be released. We recommend that users create a function for a session and call defer CompleteSession(sessionCtx) after a session is created. If session retry is needed, this function can be called multiple times if the error returned is retriable.

}

err = ExecuteActivity(sessionCtx, ProcessFileActivity, filename).Get(sessionCtx, nil)

if err != nil {

    // Session(worker) has failed or activity itself returns an error.

}

err = ExecuteActivity(sessionCtx, UploadFileActivity, filename).Get(sessionCtx, nil)

if err != nil {

    // Session(worker) has failed or activity itself returns an error.

}

err = CompleteSession(sessionCtx)

if err != nil {

    // Wrong ctx is used or failed to release session resource.

}
```

#### New Worker Options

There will be three new options available when creating a worker:

* **EnableSessionWorker**

  This flag controls whether the worker will accept activities that belong to a session.

* **SessionResourceID**

  An identifier of the resource that will be consumed if a session is executed on the worker. For example, if a worker has a GPU, and a session for training a machine learning model is executed on the worker, then some memory on the GPU will be consumed. In this case, the resourceID can be the identifier of the GPU.

**NOTE:** The user needs to ensure there’s only one worker using a certain resourceID.

* **MaxCurrentSessionExecutionSize**

  Because a session may consume some kind of resource, a user can use this option to control the maximum number of sessions running in the worker process at the same time.

## Implementation

### Session Model

All sessions are **resource specific**. This means that a session is always tied to some kind of resource (not the worker). The resource can be CPU, GPU, memory, file descriptors, etc., and we want all the activities within a session to be executed by the same worker that owns the resource. If a worker dies and restarts, as long as it owns the same resource, we can try to reestablish the session (this is future work). Also, the user needs to ensure that the resource is owned by only one worker.

Note that when creating a session, a user doesn’t need to specify the resource that is consumed by the session. This is because right now one worker owns only one resource, so there’s only one resource type on the worker. As a result, as long as the user specifies the correct tasklist in the context passed into the createSession API, the correct type of resource will be consumed (since there’s only one). Later, when a worker can support multiple types of resources, users will need to specify the resource type when creating a session.

### Workflow Context

When a user calls the CreateSession() or CreateSessionWithHostID() API, a structure that contains information about the created session will be stored in the returned context. The structure contains three pieces of information:

1. **SessionID**: a unique ID for the created session. (Exported)

2. **tasklist**: the resource specific tasklist used by this session. (Not Exported)

3. **state:** state of the session (Not Exported).  It can take one of the three values below:

   1. **Open**: when the session is created and running
   2. **Failed**: when the worker is down and the session can’t continue
   3. **Closed**: when the user closes the session through the API call and the session is successfully closed

When a user calls the CompleteSession() API, the state of the session will be changed to "Closed" in place, which is why this API doesn’t return a new context variable.

A user has no way to set the state of a session to failed. The state of a session is "failed" only when the worker executing the session is down and the change from “open” to “failed” is done in the background. Also notice that it’s possible that the worker is down, but the session state is still “Open” (since it takes some time to detect the worker failure). In this case, when a user executes an activity, the activity will still be scheduled on the resource specific tasklist. However, since no worker will poll from this task list, that activity will timeout and the user will get an error.

Note that if a user tries to call CreateSession() with a context that already has an open session, the call will return an error. The same thing happens if a call to CompleteSession() is made when there’s no open session. You can call it with a failed session, but it won’t do anything. This is allowed because the state transition to "failed" is done in the background, and user may not know the session has failed.

### Workflow Worker

When scheduling an activity, the workflow worker needs to check the context to see if this activity belongs to a session. If so and the state of that session is "open", get the session tasklist from the context and use it to override the tasklist value specified in the activity option. If on the other hand, the state is “failed”, the ExecuteActivity call will immediately fail without scheduling the activity and return an error through Future.Get().

What CreateSession() and CompleteSession() really do is **scheduling a special activity** and get some information from the worker which executes the activity.

* For CreateSession(), a special **session creation activity** will be scheduled on a global tasklist which is only used for this type of activity. During the execution of the activity, a signal containing the tasklist (depending on the session type, the tasklist will be either host specific or process specific) name will be sent back to the workflow. Then the tasklist name can be stored in the context for later use. The activity also performs heart beating throughout the whole lifetime of the session, so if the activity worker is down, the workflow can be notified and set the session state to "Failed". The CreateSession() call is considered as success when it gets the tasklist.

* For RecreateSession(), the same thing happens. The only difference is that the session creation activity will be **scheduled on the resource specific task list instead of a global one**.

* For CompleteSession(), another special **completion activity** will be scheduled on the process specific tasklist. The sole purpose of this activity is to stop the corresponding creation activity created by the CreateSession()/CreateSessionWithHostID() API call, so that the resource used by the session can be released. Note that, **the completion activity must be scheduled on the resource specific tasklist (not a global one)** since we need to make sure the completion activity and the creation activity it needs to stop are running by the same worker.

### Activity Worker

When EnableSessionWorker is set to true, two more activity workers will be started.

* Session Creation Worker: this worker polls from a global tasklist whose name is derived from the tasklist name specified in the workerOption. This worker executes only one kind of activity: the special session creation activity. This special activity serves several purposes:

   1. **Send the resource specific tasklist name** back to the workflow through signal.

   2. Keep **heart beating** to Cadence server until the end of the session.

      If the heart beating returns EntityNotExist Error, this means the workflow/session has gone and this special activity should stop to release its resources.

      If on the other hand, the worker is down, Cadence server will send a heartbeat timeout error to the workflow, so the workflow can be notified and update the session state. Later,  an ExecuteActivity() call to the failed session context will also fail.

   3. Check the number of currently running sessions on the worker. If the maximum number of concurrent sessions has been reached, return an error to fail the creation.

   One detail we need to ensure is that when the maximum number of concurrent sessions has been reached, this worker should stop polling from the global task list.

* Session Activity Worker: this worker only polls from the resource specific tasklist. There are three kinds of activities that can be run by this worker.

   1. User activities in a session that belongs to this host.

   2. Special activity to complete a session.

      The sole purpose of this activity is to stop the long running session creation activity so that new sessions can be executed.

   3. Special session creation activity scheduled by RecreateSession().

When domain failover happens, since no worker will poll from the resource specific task, all activities within a session will timeout and the user will get an error. Also, Cadence server will detect the heartbeat timeout from the session creation activity.

### Sequence Diagrams

This section illustrates the sequence diagrams for situations that may occur during the session execution.

* Here is the sequence diagram for a normal session execution.

![image alt text](1533-host-specific-tasklist-image_0.png)

* When activity worker fails during the execution.

![image alt text](1533-host-specific-tasklist-image_1.png)

* When the workflow failed or terminated during the execution.

![image alt text](1533-host-specific-tasklist-image_2.png)

* When there are too many outstanding sessions and the user is trying to create a new one. The assumption here is that each activity worker is configured to have **MaxCurrentSessionExecutionSize = 1**. Host specific tasklist and worker is omitted in the diagram.

![image alt text](1533-host-specific-tasklist-image_3.png)

* An example of CreateSessionWithHostID().

![image alt text](1533-host-specific-tasklist-image_4.png)

## Open Issues

* Support automatic session reestablishing when worker goes down.

* Add built-in query which will return a list of open sessions.

* Allow session activities to access session information such as sessionID, resourceID, etc. This information is useful for logging and debugging.

* Support multiple resources per worker and allow user to specify which type of resource a session will consume when creating the session.
