package chasm

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

const (
	visibilityComponentFqType = "core.vis"
	visibilityTaskFqType      = "core.visTask"
)

// VisibilitySearchAttributesProvider if implemented by the root Component,
// allows the CHASM framework to automatically determine, at the end of
// a transaction, if a visibility task needs to be generated to update the
// visibility record with the returned search attributes.
type VisibilitySearchAttributesProvider interface {
	SearchAttributes(Context) map[string]VisibilityValue
}

// VisibilityMemoProvider if implemented by the root Component,
// allows the CHASM framework to automatically determine, at the end of
// a transaction, if a visibility task needs to be generated to update the
// visibility record with the returned memo.
type VisibilityMemoProvider interface {
	Memo(Context) map[string]VisibilityValue
}

type Visibility struct {
	UnimplementedComponent

	Data *persistencespb.ChasmVisibilityData

	SA   Map[string, *commonpb.Payload]
	Memo Map[string, *commonpb.Payload]
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
	searchAttributes map[string]*commonpb.Payload,
	memo map[string]*commonpb.Payload,
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
	searchAttributes map[string]*commonpb.Payload,
) (bool, error) {
	updated, err := v.updatePayloadMap(mutableContext, &v.SA, searchAttributes)
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
	memo map[string]*commonpb.Payload,
) (bool, error) {
	updated, err := v.updatePayloadMap(mutableContext, &v.Memo, memo)
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
	updates map[string]*commonpb.Payload,
) (bool, error) {
	if len(updates) != 0 && *m == nil {
		*m = make(Map[string, *commonpb.Payload], len(updates))
	}

	updated := false
	for key, payload := range updates {
		currentPayloadField, exists := (*m)[key]

		if payload == nil {
			updated = updated || exists
			delete(*m, key)
			continue
		}

		if !exists {
			updated = true
			(*m)[key] = NewDataField(mutableContext, payload)
			continue
		}

		currentPayload, err := currentPayloadField.Get(mutableContext.ToImmutable())
		if err != nil {
			return false, err
		}
		if !currentPayload.Equal(payload) {
			updated = true
			(*m)[key] = NewDataField(mutableContext, payload)
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

type visibilityTaskHandler struct{}

var defaultVisibilityTaskHandler = &visibilityTaskHandler{}

func (v *visibilityTaskHandler) Validate(
	_ Context,
	component *Visibility,
	_ TaskAttributes,
	task *persistencespb.ChasmVisibilityTaskData,
) (bool, error) {
	return task.TransitionCount == component.Data.TransitionCount, nil
}

func (v *visibilityTaskHandler) Execute(
	_ context.Context,
	_ ComponentRef,
	_ TaskAttributes,
	_ *persistencespb.ChasmVisibilityTaskData,
) error {
	//nolint:forbidigo
	panic("chasm visibilityTaskExecutor should not be called directly")
}
