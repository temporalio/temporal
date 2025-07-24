package chasm

import (
	"context"

	"go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/payload"
)

const (
	// TODO: add a test for this constant
	visibilityComponentFqType = "core.vis"
	visibilityTaskFqType      = "core.visTask"
)

type CoreLibrary struct {
	UnimplementedLibrary
}

func (b *CoreLibrary) Name() string {
	return "core"
}

func (b *CoreLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{
		NewRegistrableComponent[*Visibility]("vis"),
	}
}

func (b *CoreLibrary) Tasks() []*RegistrableTask {
	return []*RegistrableTask{
		NewRegistrableSideEffectTask(
			"visTask",
			&visibilityTaskValidator{},
			&visibilityTaskExecutor{},
		),
	}
}

type Visibility struct {
	UnimplementedComponent

	Data *persistencespb.ChasmVisibilityData

	SA   Map[string, *common.Payload]
	Memo Map[string, *common.Payload]

	// TODO: Add CATMemo here for Memo added by the Components
}

func NewVisibility(
	mutableContext MutableContext,
) *Visibility {
	visibility := &Visibility{
		Data: &persistencespb.ChasmVisibilityData{
			TransitionCount: 0,
		},
	}
	visibility.GenerateTask(mutableContext)
	return visibility
}

func (v *Visibility) LifecycleState(_ Context) LifecycleState {
	return LifecycleStateRunning
}

func (v *Visibility) GetSearchAttributes(
	chasmContext Context,
) (map[string]*common.Payload, error) {
	sa := make(map[string]*common.Payload, len(v.SA))
	for key, field := range v.SA {
		value, err := field.Get(chasmContext)
		if err != nil {
			return nil, err
		}
		sa[key] = value
	}
	return sa, nil
}

func (v *Visibility) UpdateSearchAttributes(
	mutableContext MutableContext,
	updates map[string]any,
) error {
	for key, value := range updates {
		p, err := payload.Encode(value)
		if err != nil {
			return err
		}
		v.SA[key] = NewDataField(mutableContext, p)
	}
	v.GenerateTask(mutableContext)
	return nil
}

func (v *Visibility) RemoveSearchAttributes(
	mutableContext MutableContext,
	keys ...string,
) {
	for _, key := range keys {
		delete(v.SA, key)
	}
	v.GenerateTask(mutableContext)
}

func (v *Visibility) GetMemo(
	chasmContext Context,
) (map[string]*common.Payload, error) {
	memo := make(map[string]*common.Payload, len(v.Memo))
	for key, field := range v.Memo {
		value, err := field.Get(chasmContext)
		if err != nil {
			return nil, err
		}
		memo[key] = value
	}
	return memo, nil
}

func (v *Visibility) UpdateMemo(
	mutableContext MutableContext,
	updates map[string]any,
) error {
	for key, value := range updates {
		p, err := payload.Encode(value)
		if err != nil {
			return err
		}
		v.Memo[key] = NewDataField(mutableContext, p)
	}
	v.GenerateTask(mutableContext)
	return nil
}

func (v *Visibility) RemoveMemo(
	mutableContext MutableContext,
	keys ...string,
) {
	for _, key := range keys {
		delete(v.Memo, key)
	}
	v.GenerateTask(mutableContext)
}

func (v *Visibility) GenerateTask(
	mutableContext MutableContext,
) {
	v.Data.TransitionCount++
	mutableContext.AddTask(
		v,
		TaskAttributes{},
		&persistencespb.ChasmVisibilityTaskData{TransitionCount: v.Data.TransitionCount},
	)
}

func GetSearchAttribute[T any](
	chasmContext Context,
	visibility *Visibility,
	key string,
) (T, error) {
	return decodePayloadValueFromMap[T](chasmContext, visibility.SA, key)
}

func UpdateSearchAttribute[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	chasmContext MutableContext,
	visibility *Visibility,
	name string,
	value T,
) {
	p, _ := payload.Encode(value)
	visibility.SA[name] = NewDataField(chasmContext, p)
}

func GetMemo[T any](
	chasmContext Context,
	visibility *Visibility,
	key string,
) (T, error) {
	return decodePayloadValueFromMap[T](chasmContext, visibility.Memo, key)
}

func UpdateMemo[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	chasmContext MutableContext,
	visibility *Visibility,
	name string,
	value T,
) {
	p, _ := payload.Encode(value)
	visibility.Memo[name] = NewDataField(chasmContext, p)
}

func decodePayloadValueFromMap[T any](
	chasmContext Context,
	payloadMap Map[string, *common.Payload],
	key string,
) (T, error) {
	var value T
	p, err := payloadMap[key].Get(chasmContext)
	if err != nil {
		return value, err
	}
	if err = payload.Decode(p, &value); err != nil {
		return value, err
	}
	return value, nil
}

type visibilityTaskValidator struct{}

func (v *visibilityTaskValidator) Validate(
	_ Context,
	component *Visibility,
	_ TaskAttributes,
	task *persistencespb.ChasmVisibilityTaskData,
) (bool, error) {
	return task.TransitionCount == component.Data.TransitionCount, nil
}

type visibilityTaskExecutor struct{}

func (v *visibilityTaskExecutor) Execute(
	_ context.Context,
	_ ComponentRef,
	_ TaskAttributes,
	_ *persistencespb.ChasmVisibilityTaskData,
) error {
	panic("chasm visibilityTaskExecutor should not be called directly")
}
