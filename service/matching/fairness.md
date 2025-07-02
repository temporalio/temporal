
# Fair Task Reader operation

## Definitions

**Task ID**: sequential int64 assigned to tasks. Two tasks never have the same id.

**Pass**: int64 used to "spread out" tasks.

**Fair level**: tuple of <pass, id>. Fair levels are ordered lexicographically and ordered that way
by persistence.

Tasks are _written_ out of order and we rely on persistence to order them by level (most
importantly by pass).

Considering the set of tasks that were recently dispatched plus in the backlog, ordered by
level:

```
  <-------------A-----R-----------------------M------------->
```

A is the ack level (persisted): We have dispatched all tasks <= A. Therefore, we never need to
read below A, and therefore we must never write below A. A can only move forwards within the
lifetime of a partition. However, we don't write A to persistence on every dispatch, we only
update it once in a while. So one partition owner may move A forwards in memory, then crash,
then another owner may load the partition. In that case it will see an old version of A. This
is allowed to cause repeated dispatch of some tasks.

M is the maximum task level that has ever been written, inclusive (best-effort persisted).
On Cassandra, we can update M every time we write any tasks, so it should always be accurate.
On SQL it may not be.

R is the level that we have read up to and are keeping in memory, inclusive (not persisted).
I.e. We have (A, R] in memory waiting to dispatch. We may write new tasks either above or below
R.

## Operation

### Reading

When we load the task queue metadata, we get A and M. All non-dispatched backlog tasks should
be in that range (see Fencing below). Initialize R to A.

Reading without concurrent writes: Do a read for tasks with level > R, limit Bt (batch target
size). Add them to the in-memory buffer and update R to the last task read. If we get no tasks,
we can leave R where it was.


### Writing

Now add in concurrent writing:

When we write tasks, we allocate ids for them, and choose passes based on their fairness keys,
which together make the levels of the new tasks. All new task levels must be > A! (I.e. their
pass must be >= A.pass. We know their id is > A.id because ids are assigned sequentially.)

Let [Wmin, Wmax] be the range of task levels we just wrote.

If Wmin > R, then they'll show up when we read above R, so we don't have to do anything to R.

If Wmin < R, things get interesting: We're supposed to have everything below R in memory so we
don't need to read it again. But if we write below R, that breaks that assumption.

The simplest solution (plan 1) is to set R to Wmin so we'll read from there next time, so we
won't miss the new tasks. And we should drop any tasks we have in-memory that are above the new
R so the new ones get treated fairly relative to any older tasks that were in that range. If we
have more room in memory at that point, we can do a read immediately.


### Bypass optimization

Potentially dropping a bunch of tasks and rereading them on every write is inefficient. Also if
we don't have too much in memory, we shouldn't have to re-read the tasks we just wrote (even if
we don't drop anything). Instead of dropping and re-reading, we can simulate what would happen
with the add/drop/reread and add tasks to the buffer directly (plan 2):

Take the tasks in the buffer plus the tasks that were just written and sort them by level. Take
the first Bt of them (or all of them if < Bt). Set the buffer to that set. Set R to the maximum
level in that set. Discard the rest from memory.

Note that we can only do this if we've currently read to the end of the database queue,
otherwise there might be tasks in the database that should go in between our current buffer and
what we just wrote. If we're in the middle then we can merge in any new tasks below the current
end of the buffer, but we should throw out anything above that.


### GC

At any time we can issue a delete for tasks <= A. We'll do this periodically based on time or
when the number of acked tasks passes a threshold.


### Tombstone scanning

In what situations will we scan tombstones, and how many?

The Cassandra maximum tombstone limit before a read fails is 100,000, though performance may be
affected at lower levels (< 10k recommended).

First, explicit range delete tombstones:

We always delete <= A, always read > A, and A always moves forward, so in normal operation we
should never scan tombstones. We could if A moves backwards: this happens if we crash after
dispatching some tasks, before persisting the new A.

Note that we persist A when we write tasks. So we're only likely to move backwards by a
significant amount if tasks are being just dispatched without being written.

Suppose we persist A every Tp seconds, and issue a delete every Tg seconds or Ng tasks.
Considering only time: we may rescan up to Tp seconds worth of tasks, and find about Tp/Tg
tombstones in it. So we should ensure that Tg is not much smaller than Tp.

(In practice we have Tp as 60 seconds and Tg used to be 1 second, but was changed to 15 for the
priority task reader, so <= ~4 tombstones, or up to ~60 using 1 second.)

We also have to consider Ng. If the dispatch rate is R, then we'll dispatch up to Tp * R tasks.
We'd issue Tp * R / Ng deletes based on count. So we should ensure Ng is not much smaller than
R, or the maximum practical R.

(In practice, let's say the maximum R for a partition is 1000 t/s, and Tp is 60s, so we could
re-dispatch 60,000 tasks. With Ng at 100, we'd scan up to ~600 tombstones. We could increase Ng
to 1000 to reduce that to ~60.)

Next, TTLs:

Rows with TTLs become effective tombstones past their expiration time. Unfortunately we can't
control how many of these there will be in any range. So we shouldn't use TTLs on matching
tasks in the fair task table.


### Fencing

We've been assuming a single partition owner operating without interference. The owner may
change over time, so we have to consider what happens if two owners attempt to operate
simultaneously.

We also have to consider the possibility of delayed writes: if a write returns "timed out",
then we (or another owner) may observe it to take effect at some point in the future (unless
it's an LWT that we can prevent from succeeding).

For matching, if writing tasks returns "timed out", an error will be returned to the caller and
the caller will retry, or maybe give up. In any case, the caller won't assume the task has been
written. So this can lead to duplicate task dispatch, but that's not a problem.

Basically, the problematic case is the following potential sequence:

1. History calls AddTask on the old owner
2. The old owner does a write
3. Ownership changes, but the old owner doesn't realize yet
4. The new owner starts up, reads the metadata, and reads the range [A, M]
5. The new owner updates its R to the end of what it just read
6. The write now takes effect within that range
7. The old owner returns success to history, and history assumes the task has been queued
8. The new owner never re-reads that range because R has moved past it
9. The task is now effectively lost

The current solution for avoiding this is that the write in 2 is an LWT that's conditional on
the old range id. In 4, the new owner does an LWT that updates the range id and gets the
current metadata at that time. That guarantees that either the write took effect before the new
owner's update, or it will fail. If it took effect before, then the new owner will definitely
see the task when it does a read.

TODO: Is that enough to prevent all problems in the fairness schema? It seems like yes.

TODO: Are there any other ways to do it? We could just put a uuid "owner"
instead of range id, and then just allocate task ids densely.

TODO: Can we do it without lwt? That probably means time-based lease, otherwise
there's no way to force the write to fail.




## Counting

How are pass numbers assigned?

TODO



## Problems

### Ack level movement while a write is in flight

Consider:

- ack level is 10. we have tasks 11, 21, 31 in buffer.
- task writer chooses levels for some new tasks, e.g. 13, 15, 25.
- sends write to db.
- task 11 is acked. task reader sets ack level to 11.
- task 21 is acked. task reader sets ack level to 21.
- write completes. 13, 15, 25 are sent to task reader to merge into buffer.
- at this point we need to set the ack level backwards?!?

Even worse:

- ack level is 10. we have tasks 11, 21, 31 in buffer.
- task writer chooses levels for some new tasks, e.g. 13, 15, 25.
- sends write to db.
- task 11 is acked. task reader sets ack level to 11.
- task 21 is acked. task reader sets ack level to 21.
- write completes in db.
- gc kicks in: deletes <= 21.
- new tasks are now lost.

How to fix it?

At the point where task writer picks those levels, we need to add them to ack manager so that
ack manager knows it can't move the ack level further.

Does it have to add them individually, or just set a minimum "pending write level"?
Pending write level can be cleared if there is no pending write.

Let's try it with just the simplest: during a write, the ack level is pinned.
After the write and the new tasks are merged in, we unpin it, and then taskReader can try
moving the ack level.


### TTLs on tasks

We can't use TTLs on tasks so disable that for now.
We may come up with a better way to do this later.



