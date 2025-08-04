package chasm

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/payload"
)

const (
	visibilityComponentFqType = "core.vis"
	visibilityTaskFqType      = "core.visTask"
)

// CoreLibrary contains built-in components maintained as part of the CHASM framework.
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

	SA   Map[string, *commonpb.Payload]
	Memo Map[string, *commonpb.Payload]

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
) (map[string]*commonpb.Payload, error) {
	return v.getPayloadMap(chasmContext, v.SA)
}

func (v *Visibility) UpsertSearchAttributes(
	mutableContext MutableContext,
	updates map[string]any,
) error {
	return v.updatePayloadMap(mutableContext, &v.SA, updates)
}

func (v *Visibility) RemoveSearchAttributes(
	mutableContext MutableContext,
	keys ...string,
) {
	v.removeFromPayloadMap(mutableContext, v.SA, keys)
}

func (v *Visibility) GetMemo(
	chasmContext Context,
) (map[string]*commonpb.Payload, error) {
	return v.getPayloadMap(chasmContext, v.Memo)
}

func (v *Visibility) UpsertMemo(
	mutableContext MutableContext,
	updates map[string]any,
) error {
	return v.updatePayloadMap(mutableContext, &v.Memo, updates)
}

func (v *Visibility) RemoveMemo(
	mutableContext MutableContext,
	keys ...string,
) {
	v.removeFromPayloadMap(mutableContext, v.Memo, keys)
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

func (v *Visibility) getPayloadMap(
	chasmContext Context,
	m Map[string, *commonpb.Payload],
) (map[string]*commonpb.Payload, error) {
	result := make(map[string]*commonpb.Payload, len(m))
	for key, field := range m {
		value, err := field.Get(chasmContext)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

func (v *Visibility) updatePayloadMap(
	mutableContext MutableContext,
	m *Map[string, *commonpb.Payload],
	updates map[string]any,
) error {
	if len(updates) != 0 && *m == nil {
		*m = make(Map[string, *commonpb.Payload], len(updates))
	}
	for key, value := range updates {
		p, err := payload.Encode(value)
		if err != nil {
			return err
		}
		(*m)[key] = NewDataField(mutableContext, p)
	}
	v.GenerateTask(mutableContext)
	return nil
}

func (v *Visibility) removeFromPayloadMap(
	mutableContext MutableContext,
	m Map[string, *commonpb.Payload],
	keys []string,
) {
	for _, key := range keys {
		delete(m, key)
	}
	v.GenerateTask(mutableContext)
}

func GetSearchAttribute[T any](
	chasmContext Context,
	visibility *Visibility,
	key string,
) (T, error) {
	return getVisibilityPayloadValue[T](chasmContext, visibility.SA, key)
}

func UpsertSearchAttribute[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	chasmContext MutableContext,
	visibility *Visibility,
	name string,
	value T,
) {
	upsertVisibilityPayload(chasmContext, &visibility.SA, visibility, name, value)
}

func GetMemo[T any](
	chasmContext Context,
	visibility *Visibility,
	key string,
) (T, error) {
	return getVisibilityPayloadValue[T](chasmContext, visibility.Memo, key)
}

func UpsertMemo[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	chasmContext MutableContext,
	visibility *Visibility,
	name string,
	value T,
) {
	upsertVisibilityPayload(chasmContext, &visibility.Memo, visibility, name, value)
}

func upsertVisibilityPayload[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	chasmContext MutableContext,
	m *Map[string, *commonpb.Payload],
	visibility *Visibility,
	name string,
	value T,
) {
	p, _ := payload.Encode(value)
	if *m == nil {
		*m = make(Map[string, *commonpb.Payload])
	}
	(*m)[name] = NewDataField(chasmContext, p)
	visibility.GenerateTask(chasmContext)
}

func getVisibilityPayloadValue[T any](
	chasmContext Context,
	payloadMap Map[string, *commonpb.Payload],
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
	//nolint:forbidigo
	panic("chasm visibilityTaskExecutor should not be called directly")
}
