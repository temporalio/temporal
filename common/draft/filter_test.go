package draft

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	protometapb "go.temporal.io/api/protometa/v1"
	"go.temporal.io/server/common/draft/testfixture"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

func populatedMessage() *testfixture.Message {
	child := func() *testfixture.Message {
		return &testfixture.Message{Stable: "child", FeatureA: "child-a", FeatureB: "child-b"}
	}
	m := &testfixture.Message{
		Stable: "stable", FeatureA: "a", FeatureB: "b", Child: child(),
		Items: []*testfixture.Message{child()}, ByName: map[string]*testfixture.Message{"child": child()},
		OptionalA: proto.Int32(0), Choice: &testfixture.Message_ChoiceA{ChoiceA: ""},
		Raw: []byte("opaque encoded draft data"), Opaque: &anypb.Any{TypeUrl: "fixture", Value: []byte("opaque")},
	}
	m.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
	return m
}

func TestOmitSelections(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		features             []string
		a, b, childA, childB string
		child, presence      bool
	}{
		{name: "empty"},
		{name: "a", features: []string{"feature-a"}, a: "a", childA: "child-a", child: true, presence: true},
		{name: "b", features: []string{"feature-b"}, b: "b", childB: "child-b"},
		{name: "both", features: []string{"feature-a", "feature-b"}, a: "a", b: "b", childA: "child-a", childB: "child-b", child: true, presence: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := populatedMessage()
			pointer := m
			item := m.Items[0]
			reads := 0
			selection := NewSelection(func() []string { reads++; return tc.features })
			affected, err := Omit(m, selection)
			require.NoError(t, err)
			require.True(t, affected)
			require.Same(t, pointer, m)
			require.Same(t, item, m.Items[0])
			require.Equal(t, "stable", m.Stable)
			require.Equal(t, tc.a, m.FeatureA)
			require.Equal(t, tc.b, m.FeatureB)
			expectedChild := &testfixture.Message{Stable: "child", FeatureA: tc.childA, FeatureB: tc.childB}
			require.True(t, proto.Equal(expectedChild, m.Items[0]))
			require.True(t, proto.Equal(expectedChild, m.ByName["child"]))
			if tc.child {
				require.True(t, proto.Equal(expectedChild, m.Child))
			} else {
				require.Nil(t, m.Child)
			}
			require.Equal(t, tc.presence, m.OptionalA != nil)
			require.Equal(t, tc.presence, m.Choice != nil)
			require.Equal(t, []byte("opaque encoded draft data"), m.Raw)
			require.Equal(t, []byte("opaque"), m.Opaque.Value)
			require.Equal(t, protoreflect.RawFields{0x98, 0x06, 0x01}, m.ProtoReflect().GetUnknown())
			before := proto.Clone(m)
			_, err = Omit(m, selection)
			require.NoError(t, err)
			require.True(t, proto.Equal(before, m))
			require.Equal(t, 1, reads)
		})
	}
}

func TestOmitOracleAndNilElements(t *testing.T) {
	for _, features := range [][]string{nil, {"feature-a"}, {"feature-b"}, {"feature-a", "feature-b"}} {
		selection := NewSelection(func() []string { return features })
		source := populatedMessage()
		expected := proto.Clone(source)
		stripDisabled(expected.ProtoReflect(), features)
		_, err := Omit(source, selection)
		require.NoError(t, err)
		require.True(t, proto.Equal(expected, source))
	}
	selection := NewSelection(func() []string { return nil })
	var typedNil *testfixture.Message
	affected, err := Omit(typedNil, selection)
	require.NoError(t, err)
	require.True(t, affected)
	message := &testfixture.Message{Items: []*testfixture.Message{nil}, ByName: map[string]*testfixture.Message{"nil": nil}, Choice: &testfixture.Message_ChoiceChild{ChoiceChild: &testfixture.Message{Stable: "kept", FeatureA: "a"}}}
	_, err = Omit(message, selection)
	require.NoError(t, err)
	require.Nil(t, message.Items[0])
	require.Nil(t, message.ByName["nil"])
	require.Equal(t, "kept", message.GetChoiceChild().Stable)
	require.Empty(t, message.GetChoiceChild().FeatureA)
	parents := &testfixture.MapParent{Values: map[string]*testfixture.Parent{
		"message": {Message: &testfixture.Message{Stable: "stable", FeatureB: "b"}}, "nil": nil,
	}}
	_, err = Omit(parents, selection)
	require.NoError(t, err)
	require.Equal(t, "stable", parents.Values["message"].Message.Stable)
	require.Empty(t, parents.Values["message"].Message.FeatureB)
	var nilMapParent *testfixture.MapParent
	_, err = Omit(nilMapParent, selection)
	require.NoError(t, err)
}

// typeDraftFeatures returns the draft_message features of a message or enum and of the
// messages it is nested in.
func typeDraftFeatures(descriptor protoreflect.Descriptor) []string {
	var features []string
	for ; descriptor != nil; descriptor = descriptor.Parent() {
		if message, ok := descriptor.(protoreflect.MessageDescriptor); ok {
			if feature, _ := proto.GetExtension(message.Options(), protometapb.E_DraftMessage).(string); feature != "" {
				features = append(features, feature)
			}
		}
	}
	return features
}

// fieldDraftFeatures returns the features a field needs: its own and those of the
// message or enum it holds, or of a map field's value.
func fieldDraftFeatures(field protoreflect.FieldDescriptor) []string {
	var features []string
	if feature, _ := proto.GetExtension(field.Options(), protometapb.E_Draft).(string); feature != "" {
		features = append(features, feature)
	}
	if field.IsMap() {
		field = field.MapValue()
	}
	if field.Message() != nil {
		features = append(features, typeDraftFeatures(field.Message())...)
	} else if field.Enum() != nil {
		features = append(features, typeDraftFeatures(field.Enum())...)
	}
	return features
}

// stripDisabled clears every field that needs a feature outside enabled, at any depth.
func stripDisabled(m protoreflect.Message, enabled []string) {
	var disabled []protoreflect.FieldDescriptor
	m.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		for _, feature := range fieldDraftFeatures(field) {
			if !slices.Contains(enabled, feature) {
				disabled = append(disabled, field)
				return true
			}
		}
		switch {
		case field.IsList() && field.Message() != nil:
			for i := 0; i < value.List().Len(); i++ {
				stripDisabled(value.List().Get(i).Message(), enabled)
			}
		case field.IsMap() && field.MapValue().Message() != nil:
			value.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
				stripDisabled(entry.Message(), enabled)
				return true
			})
		case field.Message() != nil && !field.IsList() && !field.IsMap():
			stripDisabled(value.Message(), enabled)
		default:
			// Scalars and lists, maps of scalars hold no draft fields.
		}
		return true
	})
	for _, field := range disabled {
		m.Clear(field)
	}
}

type panickingOmitter struct{}

func (panickingOmitter) OmitDraftFields(func(string) bool) { panic("sensitive payload") }

func TestOmitLazySelectionAndRecovery(t *testing.T) {
	reads := 0
	selection := NewSelection(func() []string { reads++; return []string{"feature-a", "feature-a", "Feature-A", ""} })
	for _, value := range []any{nil, "not a proto", &testfixture.StableMessage{Stable: "stable"}} {
		affected, err := Omit(value, selection)
		require.NoError(t, err)
		require.False(t, affected)
	}
	require.Zero(t, reads)
	require.True(t, selection.Enabled("feature-a"))
	require.True(t, selection.Enabled("Feature-A"))
	require.False(t, selection.Enabled("feature-b"))
	require.Equal(t, 1, reads)
	for _, tc := range []struct {
		message   any
		selection *Selection
		name      string
		recovered string
	}{
		{panickingOmitter{}, selection, "draft.panickingOmitter", "sensitive payload"},
		{&testfixture.Message{}, NewSelection(func() []string { panic("config contents") }), "temporal.api.drafttest.v1.Message", "config contents"},
	} {
		affected, err := Omit(tc.message, tc.selection)
		require.True(t, affected)
		require.ErrorContains(t, err, tc.name)
		require.ErrorContains(t, err, tc.recovered)
	}
}

func BenchmarkOmit(b *testing.B) {
	for _, tc := range []struct {
		name    string
		message proto.Message
	}{
		{"unaffected", &testfixture.StableMessage{Stable: "stable"}},
		{"small", populatedMessage()},
		// The default BlobSizeLimitError is 2 MiB; application bytes stay opaque.
		{"blob-limit", &testfixture.Message{Stable: "stable", FeatureA: "a", Raw: make([]byte, 2*1024*1024)}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				message := proto.Clone(tc.message)
				selection := NewSelection(func() []string { return nil })
				b.StartTimer()
				if _, err := Omit(message, selection); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestOmitWholeMessages(t *testing.T) {
	for _, features := range [][]string{nil, {"feature-a"}, {"feature-b"}, {"feature-a", "feature-b"}} {
		s := NewSelection(func() []string { return features })
		m := &testfixture.DraftReferences{
			Stable: "kept", Child: &testfixture.WholeDraft{Value: "a"},
			Items:  []*testfixture.WholeDraft{{Value: "a"}},
			Values: map[string]*testfixture.WholeDraft{"a": {Value: "a"}},
			State:  testfixture.WholeDraft_ONE,
			Nested: &testfixture.WholeDraft_Nested{Value: "ab"},
			Both:   &testfixture.WholeDraft{Value: "ab"},
			Choice: &testfixture.DraftReferences_DraftChild{DraftChild: &testfixture.WholeDraft{Value: "a"}},
		}
		_, err := Omit(m, s)
		require.NoError(t, err)
		require.Equal(t, "kept", m.Stable)
		a := s.Enabled("feature-a")
		both := a && s.Enabled("feature-b")
		require.Equal(t, a, m.Child != nil)
		require.Equal(t, a, m.Items != nil)
		require.Equal(t, a, m.Values != nil)
		require.Equal(t, a, m.Choice != nil)
		require.Equal(t, a, m.State == testfixture.WholeDraft_ONE)
		require.Equal(t, both, m.Nested != nil)
		require.Equal(t, both, m.Both != nil)
	}
}
