// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/internal/goro"
)

const (
	// userDataEnabled is the default state: user data is enabled.
	userDataEnabled userDataState = iota
	// userDataClosed means the task queue is closed.
	userDataClosed
)

type (
	userDataManager interface {
		Start()
		Stop()
		WaitUntilInitialized(ctx context.Context) error
		// GetUserData returns the versioning data for this task queue. Do not mutate the returned pointer, as doing so
		// will cause cache inconsistency.
		GetUserData() (*persistencespb.VersionedTaskQueueUserData, chan struct{}, error)
		// UpdateUserData updates user data for this task queue and replicates across clusters if necessary.
		// Extra care should be taken to avoid mutating the existing data in the update function.
		UpdateUserData(ctx context.Context, options UserDataUpdateOptions, updateFn UserDataUpdateFunc) error
	}

	UserDataUpdateOptions struct {
		TaskQueueLimitPerBuildId int
		// Only perform the update if current version equals to supplied version.
		// 0 is unset.
		KnownVersion int64
	}

	// UserDataUpdateFunc accepts the current user data for a task queue and returns the updated user data, a boolean
	// indicating whether this data should be replicated, and an error.
	// Extra care should be taken to avoid mutating the current user data to avoid keeping uncommitted data in memory.
	UserDataUpdateFunc func(*persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error)

	// userDataManager is responsible for fetching and keeping user data up-to-date in-memory
	// for a given TQ partition.
	//
	// If a partition is the root of a normal (non-sticky) task queue with workflow type,
	// we say the partition "owns" user data for its task queue. All reads and writes
	// to/from the persistence layer passes through userDataManager of the owning partition.
	// All other partitions long-poll the latest user data from the owning partition.
	userDataManagerImpl struct {
		lock            sync.Mutex
		partition       tqid.Partition
		userData        *persistencespb.VersionedTaskQueueUserData
		userDataChanged chan struct{}
		userDataState   userDataState
		// only set if this partition owns user data of its task queue
		store             persistence.TaskManager
		config            *taskQueueConfig
		namespaceRegistry namespace.Registry
		logger            log.Logger
		matchingClient    matchingservice.MatchingServiceClient
		goroGroup         goro.Group
		// userDataReady is fulfilled once versioning data is fetched from the root partition. If this TQ is
		// the root partition, it is fulfilled as soon as it is fetched from db.
		userDataReady *future.FutureImpl[struct{}]
	}

	userDataState int
)

var _ userDataManager = (*userDataManagerImpl)(nil)

var (
	errUserDataNoMutateNonRoot = serviceerror.NewInvalidArgument("can only mutate user data on root workflow task queue")
	errTaskQueueClosed         = serviceerror.NewUnavailable("task queue closed")
)

func newUserDataManager(
	store persistence.TaskManager,
	matchingClient matchingservice.MatchingServiceClient,
	partition tqid.Partition,
	config *taskQueueConfig,
	logger log.Logger,
	registry namespace.Registry,
) *userDataManagerImpl {

	m := &userDataManagerImpl{
		logger:            logger,
		partition:         partition,
		config:            config,
		namespaceRegistry: registry,
		userDataChanged:   make(chan struct{}),
		matchingClient:    matchingClient,
		userDataReady:     future.NewFuture[struct{}](),
	}

	if partition.IsRoot() && partition.TaskType() == enumspb.TASK_QUEUE_TYPE_WORKFLOW {
		m.store = store
	}

	return m
}

func (m *userDataManagerImpl) Start() {
	if m.store != nil {
		m.goroGroup.Go(m.loadUserData)
	} else {
		m.goroGroup.Go(m.fetchUserData)
	}
}

func (m *userDataManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := m.userDataReady.Get(ctx)
	return err
}

func (m *userDataManagerImpl) Stop() {
	m.goroGroup.Cancel()
	// Set user data state on stop to wake up anyone blocked on the user data changed channel.
	m.setUserDataState(userDataClosed, nil)
}

// GetUserData returns the versioning data for this task queue and a channel that signals when the data has been updated.
// Do not mutate the returned pointer, as doing so will cause cache inconsistency.
// If there is no user data, this can return a nil value with no error.
func (m *userDataManagerImpl) GetUserData() (*persistencespb.VersionedTaskQueueUserData, chan struct{}, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.getUserDataLocked()
}

func (m *userDataManagerImpl) getUserDataLocked() (*persistencespb.VersionedTaskQueueUserData, chan struct{}, error) {
	switch m.userDataState {
	case userDataEnabled:
		return m.userData, m.userDataChanged, nil
	case userDataClosed:
		return nil, nil, errTaskQueueClosed
	default:
		// shouldn't happen
		return nil, nil, serviceerror.NewInternal("unexpected user data enabled state")
	}
}

func (m *userDataManagerImpl) setUserDataLocked(userData *persistencespb.VersionedTaskQueueUserData) {
	m.userData = userData
	close(m.userDataChanged)
	m.userDataChanged = make(chan struct{})
}

// Sets user data enabled/disabled and marks the future ready (if it's not ready yet).
// userDataState controls whether GetUserData return an error, and which.
// futureError is the error to set on the ready future. If this is non-nil, the task queue will
// be unloaded.
// Note that this must only be called from a single goroutine since the Ready/Set sequence is
// potentially racy otherwise.
func (m *userDataManagerImpl) setUserDataState(userDataState userDataState, futureError error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if userDataState != m.userDataState && m.userDataState != userDataClosed {
		m.userDataState = userDataState
		close(m.userDataChanged)
		m.userDataChanged = make(chan struct{})
	}

	if !m.userDataReady.Ready() {
		m.userDataReady.Set(struct{}{}, futureError)
	}
}

func (m *userDataManagerImpl) loadUserData(ctx context.Context) error {
	ctx = m.callerInfoContext(ctx)
	err := m.loadUserDataFromDB(ctx)
	m.setUserDataState(userDataEnabled, err)
	return nil
}

func (m *userDataManagerImpl) userDataFetchSource() (*tqid.NormalPartition, error) {
	switch p := m.partition.(type) {
	case *tqid.NormalPartition:
		// change to the workflow task queue
		p = p.TaskQueue().Family().TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(p.PartitionId())
		degree := m.config.ForwarderMaxChildrenPerNode()
		parent, err := p.ParentPartition(degree)
		if err == tqid.ErrNoParent { // nolint:goerr113
			// we're the root activity task queue, ask the root workflow task queue
			return p, nil
		} else if err != nil {
			// invalid degree
			return nil, err
		}
		return parent, nil
	default:
		normalQ := p.TaskQueue()
		// Sticky queues get data from their corresponding normal queue
		if normalQ.Name() == "" {
			// Older SDKs don't send the normal name. That's okay, they just can't use versioning.
			return nil, errMissingNormalQueueName
		}
		// sticky queue can only be of workflow type as of now. but to be future-proof, we make sure
		// change to workflow task queue here
		return normalQ.Family().TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), nil
	}

}

func (m *userDataManagerImpl) fetchUserData(ctx context.Context) error {
	ctx = m.callerInfoContext(ctx)

	// fetch from parent partition
	fetchSource, err := m.userDataFetchSource()
	if err != nil {
		if err == errMissingNormalQueueName { // nolint:goerr113
			// pretend we have no user data. this is a sticky queue so the only effect is that we can't
			// kick off versioned pollers.
			m.setUserDataState(userDataEnabled, nil)
		}
		return err
	}

	// hasFetchedUserData is true if we have gotten a successful reply to GetTaskQueueUserData.
	// It's used to control whether we do a long poll or a simple get.
	hasFetchedUserData := false

	op := func(ctx context.Context) error {
		knownUserData, _, _ := m.GetUserData()

		callCtx, cancel := context.WithTimeout(ctx, m.config.GetUserDataLongPollTimeout())
		defer cancel()

		res, err := m.matchingClient.GetTaskQueueUserData(callCtx, &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              m.partition.NamespaceId().String(),
			TaskQueue:                fetchSource.RpcName(),
			TaskQueueType:            fetchSource.TaskType(),
			LastKnownUserDataVersion: knownUserData.GetVersion(),
			WaitNewData:              hasFetchedUserData,
		})
		if err != nil {
			var unimplErr *serviceerror.Unimplemented
			if errors.As(err, &unimplErr) {
				// This might happen during a deployment. The older version couldn't have had any user data,
				// so we act as if it just returned an empty response and set ourselves ready.
				// Return the error so that we backoff with retry, and do not set hasFetchedUserData so that
				// we don't do a long poll next time.
				m.setUserDataState(userDataEnabled, nil)
			}
			return err
		}
		// If the root partition returns nil here, then that means our data matched, and we don't need to update.
		// If it's nil because it never existed, then we'd never have any data.
		// It can't be nil due to removing versions, as that would result in a non-nil container with
		// nil inner fields.
		if res.GetUserData() != nil {
			m.setUserDataForNonOwningPartition(res.GetUserData())
		}
		hasFetchedUserData = true
		m.setUserDataState(userDataEnabled, nil)
		return nil
	}

	minWaitTime := m.config.GetUserDataMinWaitTime

	for ctx.Err() == nil {
		start := time.Now()
		_ = backoff.ThrottleRetryContext(ctx, op, m.config.GetUserDataRetryPolicy, nil)
		elapsed := time.Since(start)

		// In general, we want to start a new call immediately on completion of the previous
		// one. But if the remote is broken and returns success immediately, we might end up
		// spinning. So enforce a minimum wait time that increases as long as we keep getting
		// very fast replies.
		if elapsed < minWaitTime {
			common.InterruptibleSleep(ctx, minWaitTime-elapsed)
			// Don't let this get near our call timeout, otherwise we can't tell the difference
			// between a fast reply and a timeout.
			minWaitTime = min(minWaitTime*2, m.config.GetUserDataLongPollTimeout()/2)
		} else {
			minWaitTime = m.config.GetUserDataMinWaitTime
		}
	}

	return ctx.Err()
}

// Loads user data from db (called only on initialization of taskQueuePartitionManager).
func (m *userDataManagerImpl) loadUserDataFromDB(ctx context.Context) error {
	response, err := m.store.GetTaskQueueUserData(ctx, &persistence.GetTaskQueueUserDataRequest{
		NamespaceID: m.partition.NamespaceId().String(),
		TaskQueue:   m.partition.TaskQueue().Name(),
	})
	if common.IsNotFoundError(err) {
		// not all task queues have user data
		response, err = &persistence.GetTaskQueueUserDataResponse{}, nil
	}
	if err != nil {
		return err
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	m.setUserDataLocked(response.UserData)

	return nil
}

// UpdateUserData updates user data for this task queue and replicates across clusters if necessary.
// Extra care should be taken to avoid mutating the existing data in the update function.
func (m *userDataManagerImpl) UpdateUserData(ctx context.Context, options UserDataUpdateOptions, updateFn UserDataUpdateFunc) error {
	if m.store == nil {
		return errUserDataNoMutateNonRoot
	}
	newData, shouldReplicate, err := m.updateUserData(ctx, updateFn, options.KnownVersion, options.TaskQueueLimitPerBuildId)
	if err != nil {
		return err
	}
	if !shouldReplicate {
		return nil
	}

	// Only replicate if namespace is global and has at least 2 clusters registered.
	ns, err := m.namespaceRegistry.GetNamespaceByID(m.partition.NamespaceId())
	if err != nil {
		return err
	}
	if ns.ReplicationPolicy() != namespace.ReplicationPolicyMultiCluster {
		return nil
	}

	_, err = m.matchingClient.ReplicateTaskQueueUserData(ctx, &matchingservice.ReplicateTaskQueueUserDataRequest{
		NamespaceId: m.partition.NamespaceId().String(),
		TaskQueue:   m.partition.TaskQueue().Name(),
		UserData:    newData.GetData(),
	})
	if err != nil {
		m.logger.Error("Failed to publish a replication task after updating task queue user data", tag.Error(err))
		return serviceerror.NewUnavailable("storing task queue user data succeeded but publishing to the namespace replication queue failed, please try again")
	}
	return err
}

// UpdateUserData allows callers to update user data (such as worker build IDs) for this task queue. The pointer passed
// to the update function is guaranteed to be non-nil.
// Note that the user data's clock may be nil and should be initialized externally where there's access to the cluster
// metadata and the cluster ID can be obtained.
//
// If knownVersion is non 0 and not equal to the current version, the update will fail.
//
// The DB write is performed remotely on an owning node for all user data updates in the namespace.
//
// On success returns a pointer to the updated data, which must *not* be mutated, and a boolean indicating whether the
// data should be replicated.
func (m *userDataManagerImpl) updateUserData(
	ctx context.Context,
	updateFn UserDataUpdateFunc,
	knownVersion int64,
	taskQueueLimitPerBuildId int,
) (*persistencespb.VersionedTaskQueueUserData, bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	userData, _, err := m.getUserDataLocked()
	if err != nil {
		return nil, false, err
	}

	preUpdateData := userData.GetData()
	preUpdateVersion := userData.GetVersion()
	if preUpdateData == nil {
		preUpdateData = &persistencespb.TaskQueueUserData{}
	}
	if knownVersion > 0 && preUpdateVersion != knownVersion {
		return nil, false, serviceerror.NewFailedPrecondition(fmt.Sprintf("user data version mismatch: requested: %d, current: %d", knownVersion, preUpdateVersion))
	}
	updatedUserData, shouldReplicate, err := updateFn(preUpdateData)
	if err != nil {
		return nil, false, err
	}

	added, removed := GetBuildIdDeltas(preUpdateData.GetVersioningData(), updatedUserData.GetVersioningData())
	if taskQueueLimitPerBuildId > 0 && len(added) > 0 {
		// We iterate here but in practice there should only be a single build Id added when the limit is enforced.
		// We do not enforce the limit when applying replication events.
		for _, buildId := range added {
			numTaskQueues, err := m.store.CountTaskQueuesByBuildId(ctx, &persistence.CountTaskQueuesByBuildIdRequest{
				NamespaceID: m.partition.NamespaceId().String(),
				BuildID:     buildId,
			})
			if err != nil {
				return nil, false, err
			}
			if numTaskQueues >= taskQueueLimitPerBuildId {
				return nil, false, serviceerror.NewFailedPrecondition(fmt.Sprintf("Exceeded max task queues allowed to be mapped to a single build ID: %d", taskQueueLimitPerBuildId))
			}
		}
	}

	_, err = m.matchingClient.UpdateTaskQueueUserData(ctx, &matchingservice.UpdateTaskQueueUserDataRequest{
		NamespaceId:     m.partition.NamespaceId().String(),
		TaskQueue:       m.partition.TaskQueue().Name(),
		UserData:        &persistencespb.VersionedTaskQueueUserData{Version: preUpdateVersion, Data: updatedUserData},
		BuildIdsAdded:   added,
		BuildIdsRemoved: removed,
	})
	var updatedVersionedData *persistencespb.VersionedTaskQueueUserData
	if err == nil {
		updatedVersionedData = &persistencespb.VersionedTaskQueueUserData{Version: preUpdateVersion + 1, Data: updatedUserData}
		m.setUserDataLocked(updatedVersionedData)
	}
	return updatedVersionedData, shouldReplicate, err
}

func (m *userDataManagerImpl) setUserDataForNonOwningPartition(userData *persistencespb.VersionedTaskQueueUserData) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.setUserDataLocked(userData)
}

func (m *userDataManagerImpl) callerInfoContext(ctx context.Context) context.Context {
	ns, _ := m.namespaceRegistry.GetNamespaceName(m.partition.NamespaceId())
	return headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(ns.String()))
}
