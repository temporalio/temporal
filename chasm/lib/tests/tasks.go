package tests

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

type (
	PayloadTTLPureTaskExecutor  struct{}
	PayloadTTLPureTaskValidator struct{}
)

func (e *PayloadTTLPureTaskExecutor) Execute(
	mutableContext chasm.MutableContext,
	store *PayloadStore,
	_ chasm.TaskAttributes,
	task *persistencespb.TestPayloadTTLPureTask,
) error {
	_, err := store.RemovePayload(mutableContext, task.PayloadKey)
	return err
}

func (v *PayloadTTLPureTaskValidator) Validate(
	_ chasm.Context,
	store *PayloadStore,
	attributes chasm.TaskAttributes,
	task *persistencespb.TestPayloadTTLPureTask,
) (bool, error) {
	return validateTask(store, attributes, task.PayloadKey)
}

type (
	PayloadTTLSideEffectTaskExecutor  struct{}
	PayloadTTLSideEffectTaskValidator struct{}
)

func (e *PayloadTTLSideEffectTaskExecutor) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *persistencespb.TestPayloadTTLSideEffectTask,
) error {
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		(*PayloadStore).RemovePayload,
		task.PayloadKey,
	)
	return err
}

func (v *PayloadTTLSideEffectTaskValidator) Validate(
	_ chasm.Context,
	store *PayloadStore,
	attributes chasm.TaskAttributes,
	task *persistencespb.TestPayloadTTLSideEffectTask,
) (bool, error) {
	return validateTask(store, attributes, task.PayloadKey)
}

func validateTask(
	store *PayloadStore,
	attributes chasm.TaskAttributes,
	payloadKey string,
) (bool, error) {
	expirationTime, ok := store.State.ExpirationTimes[payloadKey]
	if !ok {
		return false, nil
	}

	return !expirationTime.AsTime().After(attributes.ScheduledTime), nil
}
