package tests

import (
	"context"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
)

type (
	PayloadTTLPureTaskExecutor  struct{}
	PayloadTTLPureTaskValidator struct{}
)

func (e *PayloadTTLPureTaskExecutor) Execute(
	mutableContext chasm.MutableContext,
	store *PayloadStore,
	_ chasm.TaskAttributes,
	task *testspb.TestPayloadTTLPureTask,
) error {
	if err := assertContextValue(mutableContext); err != nil {
		return err
	}

	_, err := store.RemovePayload(mutableContext, task.PayloadKey)
	return err
}

func (v *PayloadTTLPureTaskValidator) Validate(
	chasmContext chasm.Context,
	store *PayloadStore,
	attributes chasm.TaskAttributes,
	task *testspb.TestPayloadTTLPureTask,
) (bool, error) {
	return validateTask(chasmContext, store, attributes, task.PayloadKey)
}

type (
	PayloadTTLSideEffectTaskExecutor  struct{}
	PayloadTTLSideEffectTaskValidator struct{}
)

func (e *PayloadTTLSideEffectTaskExecutor) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *testspb.TestPayloadTTLSideEffectTask,
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
	chasmContext chasm.Context,
	store *PayloadStore,
	attributes chasm.TaskAttributes,
	task *testspb.TestPayloadTTLSideEffectTask,
) (bool, error) {
	return validateTask(chasmContext, store, attributes, task.PayloadKey)
}

func validateTask(
	chasmContext chasm.Context,
	store *PayloadStore,
	attributes chasm.TaskAttributes,
	payloadKey string,
) (bool, error) {
	if err := assertContextValue(chasmContext); err != nil {
		return false, err
	}

	expirationTime, ok := store.State.ExpirationTimes[payloadKey]
	if !ok {
		return false, nil
	}

	return !expirationTime.AsTime().After(attributes.ScheduledTime), nil
}
