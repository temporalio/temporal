package validate_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/server/api/protohelpers/validate"
	"go.temporal.io/server/api/protohelpers/validation"
)

// TestValidate_TypedAndCrossField shows typed field validators, access to the
// parent message for cross-field checks, and the field name in errors.
func TestValidate_TypedAndCrossField(t *testing.T) {
	v := validate.CommandFieldValidators{
		CommandType: validation.Field[commandpb.Command](func(name string, ct enumspb.CommandType) error {
			if ct == enumspb.COMMAND_TYPE_UNSPECIFIED {
				return fmt.Errorf("%s is required", name)
			}
			return nil
		}),
		UserMetadata:      validation.Field[commandpb.Command](func(string, *sdkpb.UserMetadata) error { return nil }),
		EventGroupMarkers: validation.Field[commandpb.Command](func(string, []*sdkpb.EventGroupMarker) error { return nil }),
		// Cross-field: a set command_type requires matching attributes.
		Attributes: func(req *commandpb.Command, name string, attr any) error {
			if attr == nil {
				return fmt.Errorf("%s: required for command_type %v", name, req.GetCommandType())
			}
			return nil
		},
	}

	err := v.ValidateAndNormalize(&commandpb.Command{CommandType: enumspb.COMMAND_TYPE_START_TIMER})
	require.ErrorContains(t, err, "attributes: required for command_type")

	require.NoError(t, v.ValidateAndNormalize(&commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{
			StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{TimerId: "t"},
		},
	}))
}

// TestValidate_Normalize shows a field validator mutating the message in place.
func TestValidate_Normalize(t *testing.T) {
	v := validate.MemoFieldValidators{
		Fields: func(m *commonpb.Memo, _ string, _ map[string]*commonpb.Payload) error {
			delete(m.Fields, "drop-me") // normalize away a sentinel key
			return nil
		},
	}
	memo := &commonpb.Memo{Fields: map[string]*commonpb.Payload{"keep": {}, "drop-me": {}}}
	require.NoError(t, v.ValidateAndNormalize(memo))
	require.NotContains(t, memo.Fields, "drop-me")
	require.Contains(t, memo.Fields, "keep")
}

// TestValidate_Registry shows type-based dispatch through the registry.
func TestValidate_Registry(t *testing.T) {
	reg := validation.NewValidatorRegistry()
	require.NoError(t, validate.MemoFieldValidators{
		Fields: validation.Field[commonpb.Memo](func(_ string, fields map[string]*commonpb.Payload) error {
			if len(fields) > 3 {
				return fmt.Errorf("too many memo fields: %d", len(fields))
			}
			return nil
		}),
	}.RegisterValidator(reg))

	require.NoError(t, validation.ValidateAndNormalize(reg, &commonpb.Memo{Fields: map[string]*commonpb.Payload{"a": {}}}))
	require.ErrorContains(t, validation.ValidateAndNormalize(reg,
		&commonpb.Memo{Fields: map[string]*commonpb.Payload{"a": {}, "b": {}, "c": {}, "d": {}}}), "too many memo fields")

	// A type with no registered validator is reported, not silently passed.
	require.ErrorContains(t, validation.ValidateAndNormalize(reg, &commandpb.Command{}), "no validator registered")
}
