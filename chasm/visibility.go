package chasm

import (
	"context"
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/protobuf/proto"
)

const (
	UserMemoKey  = "__user__"
	ChasmMemoKey = "__chasm__"

	visibilityComponentType = "core.vis"
	visibilityTaskType      = "core.visTask"
)

var (
	visibilityComponentTypeID = generateTypeID(visibilityComponentType)
	visibilityTaskTypeID      = generateTypeID(visibilityTaskType)
)

// VisibilitySearchAttributesProvider if implemented by the root Component,
// allows the CHASM framework to automatically determine, at the end of
// a transaction, if a visibility task needs to be generated to update the
// visibility record with the returned search attributes.
type VisibilitySearchAttributesProvider interface {
	SearchAttributes(Context) []SearchAttributeKeyValue
}

// VisibilityMemoProvider if implemented by the root Component,
// allows the CHASM framework to automatically determine, at the end of
// a transaction, if a visibility task needs to be generated to update the
// visibility record with the returned memo.
type VisibilityMemoProvider interface {
	Memo(Context) proto.Message
}

// VisibilitySearchAttributesMapper is a mapper for CHASM search attributes.
type VisibilitySearchAttributesMapper struct {
	aliasToField map[string]string
	fieldToAlias map[string]string
	saTypeMap    map[string]enumspb.IndexedValueType

	systemAliasToField map[string]string
}

// Alias returns the alias for a given field.
func (v *VisibilitySearchAttributesMapper) Alias(field string) (string, error) {
	if v == nil {
		return "", serviceerror.NewInvalidArgument("visibility search attributes mapper not defined")
	}
	alias, ok := v.fieldToAlias[field]
	if !ok {
		return "", serviceerror.NewInvalidArgument(fmt.Sprintf("visibility search attributes mapper has no registered field %q", field))
	}
	return alias, nil
}

// Field returns the field for a given alias.
func (v *VisibilitySearchAttributesMapper) Field(alias string) (string, error) {
	if v == nil {
		return "", serviceerror.NewInvalidArgument("visibility search attributes mapper not defined")
	}
	if field, ok := v.aliasToField[alias]; ok {
		return field, nil
	}
	if field, ok := v.resolveSystemAlias(alias); ok {
		return field, nil
	}
	return "", serviceerror.NewInvalidArgument(fmt.Sprintf("visibility search attributes mapper has no registered alias %q", alias))
}

// resolveSystemAlias resolves a system search attribute alias to its field name.
// It handles the `Temporal` prefix variations (e.g., "ScheduleId" and "TemporalScheduleId").
func (v *VisibilitySearchAttributesMapper) resolveSystemAlias(alias string) (string, bool) {
	if v.systemAliasToField == nil {
		return "", false
	}
	if field, ok := v.systemAliasToField[alias]; ok {
		return field, true
	}
	// Try without the `Temporal` prefix.
	if strings.HasPrefix(alias, sadefs.ReservedPrefix) {
		withoutPrefix := alias[len(sadefs.ReservedPrefix):]
		if field, ok := v.systemAliasToField[withoutPrefix]; ok {
			return field, true
		}
	}
	// Try with the `Temporal` prefix.
	withPrefix := sadefs.ReservedPrefix + alias
	if field, ok := v.systemAliasToField[withPrefix]; ok {
		return field, true
	}
	return "", false
}

// SATypeMap returns the type map for the CHASM search attributes.
func (v *VisibilitySearchAttributesMapper) SATypeMap() map[string]enumspb.IndexedValueType {
	if v == nil {
		return nil
	}
	return v.saTypeMap
}

// ValueType returns the type of a CHASM search attribute field.
// Returns an error if the field is not found in the type map.
func (v *VisibilitySearchAttributesMapper) ValueType(fieldName string) (enumspb.IndexedValueType, error) {
	if v == nil {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, serviceerror.NewInvalidArgument("visibility search attributes mapper not defined")
	}
	typ, ok := v.saTypeMap[fieldName]
	if !ok {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, serviceerror.NewInvalidArgumentf("visibility search attributes mapper has no registered field %q", fieldName)
	}
	return typ, nil
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
) *Visibility {
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
	return visibility
}

func (v *Visibility) LifecycleState(_ Context) LifecycleState {
	return LifecycleStateRunning
}

func (v *Visibility) GetSearchAttributes(
	chasmContext Context,
) map[string]*commonpb.Payload {
	sa, _ := v.SA.TryGet(chasmContext)
	// nil check handled by the proto getter.
	return sa.GetIndexedFields()
}

func (v *Visibility) SetSearchAttributes(
	mutableContext MutableContext,
	customSearchAttributes map[string]*commonpb.Payload,
) {
	currentSA, ok := v.SA.TryGet(mutableContext)

	if !ok {
		currentSA = &commonpb.SearchAttributes{}
		v.SA = NewDataField(mutableContext, currentSA)
	}

	currentSA.IndexedFields = payload.MergeMapOfPayload(
		currentSA.GetIndexedFields(),
		customSearchAttributes,
	)
	v.generateTask(mutableContext)
}

func (v *Visibility) GetMemo(
	chasmContext Context,
) map[string]*commonpb.Payload {
	memo, _ := v.Memo.TryGet(chasmContext)
	// nil check handled by the proto getter.
	return memo.GetFields()
}

func (v *Visibility) SetMemo(
	mutableContext MutableContext,
	customMemo map[string]*commonpb.Payload,
) {
	currentMemo, ok := v.Memo.TryGet(mutableContext)

	if !ok {
		currentMemo = &commonpb.Memo{}
		v.Memo = NewDataField(mutableContext, currentMemo)
	}

	currentMemo.Fields = payload.MergeMapOfPayload(
		currentMemo.GetFields(),
		customMemo,
	)
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
