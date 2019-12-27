# Design doc: Cadence NDC

Author: Cadence Team

Last updated: Dec 2019

Reference: [#2290](https://github.com/uber/cadence/issues/2290)


## Abstract

Cadence Cross DC is a feature which asynchronously replicates workflows from active data center to other passive data centers, for backup & state reconstruction. When necessary, customer can failover to any of the data centers which have the backup for high availability.

Cadence Cross DC is an AP (in terms of CAP).

This doc explains the high level concepts about Cadence Cross DC (mainly the N data center version).


## Newly Introduced Components

1. Version
2. Version history
3. Workflow history conflict resolution
4. Zombie workflows
5. Workflow task processing

### Version

Version is a newly introduced concept in Cadence Cross DC which describes the chronological order of events (per customer domain).

Cadence Cross DC is AP, all domain change events & workflow history events are replicated asynchronously for high throughput. This means that data across data centers are not strongly consistent. To guarantee that domain data & workflow data will achieve eventual consistency (especially when there is data conflict during a failover), version is introduced and attached to customers' domains. All workflow history events generated in a domain will also come with the version in that domain.

All participating data centers are pre-configured with a unique initial version, and a shared version increment:

* `initial version < shared version increment`

When performing failover for one domain from one data center to another data center, the version attached to the domain will be changed by the following rule:

* for all versions which follow `version % (shared version increment) == (active data centers' initial version)`, find the smallest version which has `version >= old version in domain`

When there is a data conflict, comparison will be made and workflow history events with the highest version will win.

When a data center is trying to mutate a workflow, version will be checked. A data center can mutate a workflow if and only if

* version in the domain belongs to this data center, i.e.
  `(version in domain) % (shared version increment) == (this data centers' initial version)`
* the version of this workflow's last event is equal or less then version in domain, i.e.
  `(last event's version) <= (version in domain)`

Domain version change example:

* Data center A comes with initial version: 1
* Data center B comes with initial version: 2
* Shared version increment: 10

T = 0: domain α is registered, with active data center set to data center A
```
domain α's version is 1
all workflows events generated within this domain, will come with version 1
```

T = 1: domain β is registered, with active data center set to data center B
```
domain β's version is 2
all workflows events generated within this domain, will come with version 2
```

T = 2: domain α is updated to with active data center set to data center B
```
domain α's version is 2
all workflows events generated within this domain, will come with version 2
```

T = 3: domain β is updated to with active data center set to data center A
```
domain β's version is 11
all workflows events generated within this domain, will come with version 11
```

### Version History

Version history is a newly introduced concept which provides high level summary about version information of workflow history.

Whenever there is a new workflow history event generated, the version from domain will be attached. Workflow mutable state will keep track of all history events & corresponding version.

Example, version history without data conflict:

* Data center A comes with initial version: 1
* Data center B comes with initial version: 2
* Shared version increment: 10

T = 0:  adding event with event ID == 1 & version == 1

View in both data center A & B
```
| -------- | ------------- | --------------- | ------- |
| Events                   | Version History           |
| -------- | ------------- | --------------- | ------- |
| Event ID | Event Version | Event ID        | Version |
| -------- | ------------- | --------------- | ------- |
| 1        | 1             | 1               | 1       |
| -------- | ------------- | --------------- | ------- |
```

T = 1:  adding event with event ID == 2 & version == 1

View in both data center A & B
```
| -------- | ------------- | --------------- | ------- |
| Events                   | Version History           |
| -------- | ------------- | --------------- | ------- |
| Event ID | Event Version | Event ID        | Version |
| -------- | ------------- | --------------- | ------- |
| 1        | 1             | 2               | 1       |
| 2        | 1             |                 |         |
| -------- | ------------- | --------------- | ------- |
```

T = 2:  adding event with event ID == 3 & version == 1

View in both data center A & B
```
| -------- | ------------- | --------------- | ------- |
| Events                   | Version History           |
| -------- | ------------- | --------------- | ------- |
| Event ID | Event Version | Event ID        | Version |
| -------- | ------------- | --------------- | ------- |
| 1        | 1             | 3               | 1       |
| 2        | 1             |                 |         |
| 3        | 1             |                 |         |
| -------- | ------------- | --------------- | ------- |
```

T = 3:  domain failover triggered, domain version is now 2
        adding event with event ID == 4 & version == 2

View in both data center A & B
```
| -------- | ------------- | --------------- | ------- |
| Events                   | Version History           |
| -------- | ------------- | --------------- | ------- |
| Event ID | Event Version | Event ID        | Version |
| -------- | ------------- | --------------- | ------- |
| 1        | 1             | 3               | 1       |
| 2        | 1             | 4               | 2       |
| 3        | 1             |                 |         |
| 4        | 2             |                 |         |
| -------- | ------------- | --------------- | ------- |
```

T = 4:  adding event with event ID == 5 & version == 2

View in both data center A & B
```
| -------- | ------------- | --------------- | ------- |
| Events                   | Version History           |
| -------- | ------------- | --------------- | ------- |
| Event ID | Event Version | Event ID        | Version |
| -------- | ------------- | --------------- | ------- |
| 1        | 1             | 3               | 1       |
| 2        | 1             | 5               | 2       |
| 3        | 1             |                 |         |
| 4        | 2             |                 |         |
| 5        | 2             |                 |         |
| -------- | ------------- | --------------- | ------- |
```

Since Cadence is AP, during failover (change of active data center of a domain), there exist case that more than one data center can modify a workflow, causing divergence of workflow history. Below shows how version history will look like under such conditions.

Example, version history with data conflict:

Below will show version history of the same workflow in 2 different data centers.

* Data center A comes with initial version: 1
* Data center B comes with initial version: 2
* Data center C comes with initial version: 3
* Shared version increment: 10

T = 0:

View in both data center B & C
```
| -------- | ------------- | --------------- | ------- |
| Events                   | Version History           |
| -------- | ------------- | --------------- | ------- |
| Event ID | Event Version | Event ID        | Version |
| -------- | ------------- | --------------- | ------- |
| 1        | 1             | 2               | 1       |
| 2        | 1             | 3               | 2       |
| 3        | 2             |                 |         |
| -------- | ------------- | --------------- | ------- |
```

T = 1: adding event with event ID == 4 & version == 2 in data center B
```
| -------- | ------------- | --------------- | ------- |
| Events                   | Version History           |
| -------- | ------------- | --------------- | ------- |
| Event ID | Event Version | Event ID        | Version |
| -------- | ------------- | --------------- | ------- |
| 1        | 1             | 2               | 1       |
| 2        | 1             | 4               | 2       |
| 3        | 2             |                 |         |
| 4        | 2             |                 |         |
| -------- | ------------- | --------------- | ------- |
```

T = 1: domain failover to data center C, adding event with event ID == 4 & version == 3 in data center C
```
| -------- | ------------- | --------------- | ------- |
| Events                   | Version History           |
| -------- | ------------- | --------------- | ------- |
| Event ID | Event Version | Event ID        | Version |
| -------- | ------------- | --------------- | ------- |
| 1        | 1             | 2               | 1       |
| 2        | 1             | 3               | 2       |
| 3        | 2             | 4               | 3       |
| 4        | 3             |                 |         |
| -------- | ------------- | --------------- | ------- |
```

T = 2:  replication task from data center C arrives in data center B

Note: below are a tree structures
```
                | -------- | ------------- |
                | Events                   |
                | -------- | ------------- |
                | Event ID | Event Version |
                | -------- | ------------- |
                | 1        | 1             |
                | 2        | 1             |
                | 3        | 2             |
                | -------- | ------------- |
                           |
           | ------------- | ------------ |
           |                              |
| -------- | ------------- |   | -------- | ------------- |
| Event ID | Event Version |   | Event ID | Event Version |
| -------- | ------------- |   | -------- | ------------- |
| 4        | 2             |   | 4        | 3             |
| -------- | ------------- |   | -------- | ------------- |

          | --------------- | ------- |
          | Version History           |
          | --------------- | ------- |
          | Event ID        | Version |
          | --------------- | ------- |
          | 2               | 1       |
          | 3               | 2       |
          | --------------- | ------- |
                            |
                  | ------- | ------------------- |
                  |                               |
| --------------- | ------- |   | --------------- | ------- |
| Event ID        | Version |   | Event ID        | Version |
| --------------- | ------- |   | --------------- | ------- |
| 4               | 2       |   | 4               | 3       |
| --------------- | ------- |   | --------------- | ------- |
```

T = 2:  replication task from data center B arrives in data center C, same as above


### Workflow History Conflict Resolution

When a workflow encounters divergence of workflow history, proper conflict resolution should be applied.

In Cadence NDC, workflow history events are modeled as a tree, as shown in the second example in [### Version History].

Workflows which encounters divergence will have more than one history branches. Among all history branches, the history branch with the highest version is considered as the `current branch` and workflow mutable state is a summary of the current branch. Whenever there is a switch between workflow history branches, a complete rebuild of workflow mutable state will occur.


### Zombie Workflows

There is an existing contract that for any domain & workflow ID combination, there can be at most one run (domain & workflow ID & run ID) open / running.

Cadence NDC aims to keep the workflow state as up-to-date as possible among all participating data centers.

Due to the nature of Cadence NDC, i.e. workflow history events are replicated asynchronously, different run (same domain & workflow ID) can arrive at target data center at different times, sometimes out of order, as shown below:

```
| ------------- |          | ------------- |          | ------------- |
| Data Center A |          | Network Layer |          | Data Center B |
| ------------- |          | ------------- |          | ------------- |
        |                          |                          |
        | Run 1 Replication Events |                          |
        | -----------------------> |                          |
        |                          |                          |
        | Run 2 Replication Events |                          |
        | -----------------------> |                          |
        |                          |                          |
        |                          |                          |
        |                          |                          |
        |                          | Run 2 Replication Events |
        |                          | -----------------------> |
        |                          |                          |
        |                          | Run 1 Replication Events |
        |                          | -----------------------> |
        |                          |                          |
| ------------- |          | ------------- |          | ------------- |
| Data Center A |          | Network Layer |          | Data Center B |
| ------------- |          | ------------- |          | ------------- |
```

Since run 2 appears in data center B first, run 1 cannot be replicated as runnable due to rule `at most one run open` (see above), thus, `zombie` workflow state is introduced. Zombie state indicates a workflow which cannot be actively mutated by a data center (assuming corresponding domain is active in this data center). A zombie workflow can only be changed by replication task.

Run 1 will be replicated similar to run 2, except run 1's workflow state will be zombie before run 1 reaches completion.


### Workflow Task Processing

In the context of Cadence NDC, workflow mutable state is an entity which tracks all pending tasks. Prior to the introduction of Cadence NDC, workflow history events are from a single branch, and Cadence server will only append new events to workflow history.

After the introduction of Cadence NDC, it is possible that a workflow can have multiple workflow history branches. Tasks generated according to one history branch maybe invalidated by history branch switching during conflict resolution.

Example:

T = 0: task A is generated according to event ID: 4, version: 2
```
| -------- | ------------- |
| Events                   |
| -------- | ------------- |
| Event ID | Event Version |
| -------- | ------------- |
| 1        | 1             |
| 2        | 1             |
| 3        | 2             |
| -------- | ------------- |
           |
           |
| -------- | ------------- |
| Event ID | Event Version |
| -------- | ------------- |
| 4        | 2             | <-- task A belongs to this event
| -------- | ------------- |
```

T = 1: conflict resolution happens, workflow mutable state is rebuilt and history event ID: 4, version: 3 is written down to persistence
```
                | -------- | ------------- |
                | Events                   |
                | -------- | ------------- |
                | Event ID | Event Version |
                | -------- | ------------- |
                | 1        | 1             |
                | 2        | 1             |
                | 3        | 2             |
                | -------- | ------------- |
                           |
           | ------------- | -------------------------------------------- |
           |                                                              |
| -------- | ------------- |                                   | -------- | ------------- |
| Event ID | Event Version |                                   | Event ID | Event Version |
| -------- | ------------- |                                   | -------- | ------------- |
| 4        | 2             | <-- task A belongs to this event  | 4        | 3             | <-- current branch / mutable state
| -------- | ------------- |                                   | -------- | ------------- |
```

T = 2: task A is loaded.

At this time, due to the rebuilt of workflow mutable state (conflict resolution), task A is no longer relevant (task A's corresponding event belongs to non-current branch). Task processing logic will verify both the event ID and version of the task against corresponding workflow mutable state, then discard task A.

