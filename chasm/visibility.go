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

// VisibilitySearchAttributesProvider if implemented by the root Component,
// allows the CHASM framework to automatically determine, at the end of
// a transaction, if a visibility task needs to be generated to update the
// visibility record with the returned search attributes.
//
// TODO: Improve this interface after support registering CHASM search attributes.
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

	SA   Field[*commonpb.SearchAttributes]
	Memo Field[*commonpb.Memo]
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
	customSearchAttributes map[string]*commonpb.Payload,
	customMemo map[string]*commonpb.Payload,
) (*Visibility, error) {
	visibility := &Visibility{
		Data: &persistencespb.ChasmVisibilityData{
			TransitionCount: 0,
		},
		SA: NewDataField(
			mutableContext,
			&commonpb.SearchAttributes{IndexedFields: customSearchAttributes},
		),
		Memo: NewDataField(
			mutableContext,
			&commonpb.Memo{Fields: customMemo},
		),
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
	sa, err := v.SA.Get(chasmContext)
	if err != nil {
		return nil, err
	}
	return sa.GetIndexedFields(), nil
}

func (v *Visibility) SetSearchAttributes(
	mutableContext MutableContext,
	customSearchAttributes map[string]*commonpb.Payload,
) error {
	currentSA, err := v.SA.Get(mutableContext)
	if err != nil {
		return err
	}

	if currentSA == nil {
		currentSA = &commonpb.SearchAttributes{}
		v.SA = NewDataField(mutableContext, currentSA)
	}

	currentSA.IndexedFields = payload.MergeMapOfPayload(
		currentSA.GetIndexedFields(),
		customSearchAttributes,
	)
	v.generateTask(mutableContext)

	return nil
}

func (v *Visibility) GetMemo(
	chasmContext Context,
) (map[string]*commonpb.Payload, error) {
	memo, err := v.Memo.Get(chasmContext)
	if err != nil {
		return nil, err
	}
	return memo.GetFields(), nil
}

func (v *Visibility) SetMemo(
	mutableContext MutableContext,
	customMemo map[string]*commonpb.Payload,
) error {
	currentMemo, err := v.Memo.Get(mutableContext)
	if err != nil {
		return err
	}

	if currentMemo == nil {
		currentMemo = &commonpb.Memo{}
		v.Memo = NewDataField(mutableContext, currentMemo)
	}

	currentMemo.Fields = payload.MergeMapOfPayload(
		currentMemo.GetFields(),
		customMemo,
	)
	v.generateTask(mutableContext)

	return nil
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
