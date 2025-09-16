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

// VisibilitySearchAttributesProvider ...
// TODO: add comments
type VisibilitySearchAttributesProvider interface {
	SearchAttributes(Context) map[string]any
}

// VisibilityMemoProvider ...
// TODO: add comments
type VisibilityMemoProvider interface {
	Memo(Context) map[string]any
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

	visibility.generateTask(mutableContext)
	return visibility
}

func NewVisibilityWithData(
	mutableContext MutableContext,
	searchAttributes map[string]any,
	memo map[string]any,
) (*Visibility, error) {
	visibility := &Visibility{
		Data: &persistencespb.ChasmVisibilityData{
			TransitionCount: 0,
		},
	}

	if _, err := visibility.updatePayloadMap(
		mutableContext,
		&visibility.SA,
		searchAttributes,
	); err != nil {
		return nil, err
	}

	if _, err := visibility.updatePayloadMap(
		mutableContext,
		&visibility.Memo,
		memo,
	); err != nil {
		return nil, err
	}

	visibility.generateTask(mutableContext)
	return visibility, nil
}

func (v *Visibility) LifecycleState(_ Context) LifecycleState {
	return LifecycleStateRunning
}

func (v *Visibility) GetSearchAttributes(
	chasmContext Context,
) (map[string]*commonpb.Payload, error) {
	return v.getPayloadMap(chasmContext, v.SA)
}

func (v *Visibility) SetSearchAttributes(
	mutableContext MutableContext,
	updates map[string]any,
) (bool, error) {
	updated, err := v.updatePayloadMap(mutableContext, &v.SA, updates)
	if updated {
		v.generateTask(mutableContext)
	}
	return updated, err
}

func (v *Visibility) RemoveSearchAttributes(
	mutableContext MutableContext,
	keys ...string,
) {
	v.removeFromPayloadMap(v.SA, keys)
	v.generateTask(mutableContext)
}

func (v *Visibility) GetMemo(
	chasmContext Context,
) (map[string]*commonpb.Payload, error) {
	return v.getPayloadMap(chasmContext, v.Memo)
}

func (v *Visibility) SetMemo(
	mutableContext MutableContext,
	updates map[string]any,
) (bool, error) {
	updated, err := v.updatePayloadMap(mutableContext, &v.Memo, updates)
	if updated {
		v.generateTask(mutableContext)
	}
	return updated, err
}

func (v *Visibility) RemoveMemo(
	mutableContext MutableContext,
	keys ...string,
) {
	v.removeFromPayloadMap(v.Memo, keys)
	v.generateTask(mutableContext)
}

func (v *Visibility) generateTask(
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
) (bool, error) {
	if len(updates) != 0 && *m == nil {
		*m = make(Map[string, *commonpb.Payload], len(updates))
	}

	updated := false
	for key, value := range updates {
		currentPayloadField, exists := (*m)[key]

		if value == nil {
			updated = updated || exists
			delete(*m, key)
			continue
		}

		p, err := payload.Encode(value)
		if err != nil {
			return false, err
		}

		if !exists {
			updated = true
			(*m)[key] = NewDataField(mutableContext, p)
			continue
		}

		currentPayload, err := currentPayloadField.Get(mutableContext.ToImmutable())
		if err != nil {
			return false, err
		}
		if !currentPayload.Equal(p) {
			updated = true
			(*m)[key] = NewDataField(mutableContext, p)
		}
	}
	return updated, nil
}

func (v *Visibility) removeFromPayloadMap(
	m Map[string, *commonpb.Payload],
	keys []string,
) {
	for _, key := range keys {
		delete(m, key)
	}
}

func GetSearchAttribute[T any](
	chasmContext Context,
	visibility *Visibility,
	key string,
) (T, error) {
	return getVisibilityPayloadValue[T](chasmContext, visibility.SA, key)
}

// TODO: only update if value is different and return bool to indicate if updated
func SetSearchAttribute[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	mutableContext MutableContext,
	visibility *Visibility,
	name string,
	value T,
) {
	upsertVisibilityPayload(mutableContext, &visibility.SA, visibility, name, value)
}

func GetMemo[T any](
	chasmContext Context,
	visibility *Visibility,
	key string,
) (T, error) {
	return getVisibilityPayloadValue[T](chasmContext, visibility.Memo, key)
}

// TODO: only update if value is different and return bool to indicate if updated
func SetMemo[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	mutableContext MutableContext,
	visibility *Visibility,
	name string,
	value T,
) {
	upsertVisibilityPayload(mutableContext, &visibility.Memo, visibility, name, value)
}

func upsertVisibilityPayload[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	mutableContext MutableContext,
	m *Map[string, *commonpb.Payload],
	visibility *Visibility,
	name string,
	value T,
) {
	p, _ := payload.Encode(value)
	if *m == nil {
		*m = make(Map[string, *commonpb.Payload])
	}
	(*m)[name] = NewDataField(mutableContext, p)
	visibility.generateTask(mutableContext)
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
