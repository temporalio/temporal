package searchattribute

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func Test_IsValid(t *testing.T) {
	r := require.New(t)
	typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}

	isDefined := typeMap.IsDefined("RunId")
	r.True(isDefined)
	isDefined = typeMap.IsDefined("TemporalChangeVersion")
	r.True(isDefined)
	isDefined = typeMap.IsDefined("key1")
	r.True(isDefined)

	isDefined = NameTypeMap{}.IsDefined("key1")
	r.False(isDefined)
	isDefined = typeMap.IsDefined("key4")
	r.False(isDefined)
	isDefined = typeMap.IsDefined("NamespaceId")
	r.False(isDefined)
}

func Test_GetType(t *testing.T) {
	t.Run("CustomAndDefaultSystemPredefined", func(t *testing.T) {
		r := require.New(t)
		typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"key2": enumspb.INDEXED_VALUE_TYPE_INT,
			"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		}}

		// Custom attributes resolve.
		ivt, err := typeMap.GetType("key1")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)
		ivt, err = typeMap.GetType("key2")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_INT, ivt)
		ivt, err = typeMap.GetType("key3")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, ivt)

		// Default system SA resolves.
		ivt, err = typeMap.GetType("RunId")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)

		// Default predefined SA resolves.
		ivt, err = typeMap.GetType("TemporalChangeVersion")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, ivt)

		// NamespaceId is not a public SA.
		ivt, err = typeMap.GetType("NamespaceId")
		r.Error(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)
	})

	t.Run("ErrorCases", func(t *testing.T) {
		r := require.New(t)
		typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		}}

		// Unknown key on empty map.
		ivt, err := NameTypeMap{}.GetType("key1")
		r.Error(err)
		r.ErrorIs(err, sadefs.ErrInvalidName)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)

		// Unknown key on populated map.
		ivt, err = typeMap.GetType("nonexistent")
		r.Error(err)
		r.ErrorIs(err, sadefs.ErrInvalidName)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ivt)
	})

	t.Run("OverriddenSystemAndPredefined", func(t *testing.T) {
		r := require.New(t)
		typeMap := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"MySys": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"MyPred": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"MyCustom": enumspb.INDEXED_VALUE_TYPE_TEXT,
			},
		}

		// All three categories resolve.
		ivt, err := typeMap.GetType("MySys")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_INT, ivt)

		ivt, err = typeMap.GetType("MyPred")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, ivt)

		ivt, err = typeMap.GetType("MyCustom")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)

		// Default system/predefined SAs are no longer found after override.
		_, err = typeMap.GetType("RunId")
		r.ErrorIs(err, sadefs.ErrInvalidName)
		_, err = typeMap.GetType("TemporalChangeVersion")
		r.ErrorIs(err, sadefs.ErrInvalidName)
	})

	t.Run("CustomTakesPriorityOverPredefinedAndSystem", func(t *testing.T) {
		r := require.New(t)
		// Same key in all three categories — custom should win.
		typeMap := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"Shared": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"Shared": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"Shared": enumspb.INDEXED_VALUE_TYPE_TEXT,
			},
		}
		ivt, err := typeMap.GetType("Shared")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)
	})

	t.Run("PredefinedTakesPriorityOverSystem", func(t *testing.T) {
		r := require.New(t)
		// Same key in system and predefined — predefined should win.
		typeMap := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"Shared": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"Shared": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
		}
		ivt, err := typeMap.GetType("Shared")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, ivt)
	})
}

func Test_System(t *testing.T) {
	t.Run("DefaultSystemAndPredefined", func(t *testing.T) {
		r := require.New(t)
		typeMap := NewNameTypeMap(map[string]enumspb.IndexedValueType{
			"MyCustom": enumspb.INDEXED_VALUE_TYPE_TEXT,
		})

		sys := typeMap.System()
		// Contains default system SAs.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, sys["RunId"])
		// Contains default predefined SAs.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, sys["TemporalChangeVersion"])
		// Does not contain custom SAs.
		_, hasCustom := sys["MyCustom"]
		r.False(hasCustom)
	})

	t.Run("OverriddenSystemAndPredefined", func(t *testing.T) {
		r := require.New(t)
		typeMap := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"MySys": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"MyPred": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"MyCustom": enumspb.INDEXED_VALUE_TYPE_TEXT,
			},
		}

		sys := typeMap.System()
		r.Equal(enumspb.INDEXED_VALUE_TYPE_INT, sys["MySys"])
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, sys["MyPred"])
		r.Len(sys, 2)
		// Custom SAs excluded.
		_, hasCustom := sys["MyCustom"]
		r.False(hasCustom)
	})

	t.Run("PredefinedOverridesSystemOnConflict", func(t *testing.T) {
		r := require.New(t)
		// System() copies system first, then predefined — predefined wins on conflict.
		typeMap := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"Shared": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"Shared": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
		}
		sys := typeMap.System()
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, sys["Shared"])
	})

	t.Run("EmptyMap", func(t *testing.T) {
		r := require.New(t)
		// Zero-value NameTypeMap falls back to global defaults.
		sys := NameTypeMap{}.System()
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, sys["RunId"])
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, sys["TemporalChangeVersion"])
	})
}

func Test_Custom(t *testing.T) {
	t.Run("ReturnsCustomOnly", func(t *testing.T) {
		r := require.New(t)
		custom := map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		}
		typeMap := NewNameTypeMap(custom)
		r.Equal(custom, typeMap.Custom())
	})

	t.Run("NilWhenNoCustom", func(t *testing.T) {
		r := require.New(t)
		typeMap := NewNameTypeMap(nil)
		r.Nil(typeMap.Custom())
	})

	t.Run("EmptyMap", func(t *testing.T) {
		r := require.New(t)
		r.Nil(NameTypeMap{}.Custom())
	})
}

func Test_All(t *testing.T) {
	t.Run("DefaultSystemAndPredefined", func(t *testing.T) {
		r := require.New(t)
		custom := map[string]enumspb.IndexedValueType{
			"MyCustom": enumspb.INDEXED_VALUE_TYPE_TEXT,
		}
		typeMap := NewNameTypeMap(custom)

		all := typeMap.All()
		// Contains default system SAs.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, all["RunId"])
		// Contains default predefined SAs.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, all["TemporalChangeVersion"])
		// Contains custom SAs.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, all["MyCustom"])
	})

	t.Run("OverriddenSystemAndPredefined", func(t *testing.T) {
		r := require.New(t)
		typeMap := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"MySys": enumspb.INDEXED_VALUE_TYPE_INT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"MyPred": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"MyCustom": enumspb.INDEXED_VALUE_TYPE_TEXT,
			},
		}

		all := typeMap.All()
		r.Len(all, 3)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_INT, all["MySys"])
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, all["MyPred"])
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, all["MyCustom"])

		// Default SAs are not present.
		_, hasRunID := all["RunId"]
		r.False(hasRunID)
	})

	t.Run("CustomOverridesPredefinedOverridesSystem", func(t *testing.T) {
		r := require.New(t)
		// All() copies system, then predefined, then custom — last write wins.
		typeMap := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"SysOnly":   enumspb.INDEXED_VALUE_TYPE_INT,
				"SysPred":   enumspb.INDEXED_VALUE_TYPE_INT,
				"SysCustom": enumspb.INDEXED_VALUE_TYPE_INT,
				"AllThree":  enumspb.INDEXED_VALUE_TYPE_INT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"PredOnly":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
				"SysPred":    enumspb.INDEXED_VALUE_TYPE_DOUBLE,
				"PredCustom": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
				"AllThree":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"CustomOnly": enumspb.INDEXED_VALUE_TYPE_TEXT,
				"SysCustom":  enumspb.INDEXED_VALUE_TYPE_TEXT,
				"PredCustom": enumspb.INDEXED_VALUE_TYPE_TEXT,
				"AllThree":   enumspb.INDEXED_VALUE_TYPE_TEXT,
			},
		}

		all := typeMap.All()
		// Unique keys resolve to their own type.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_INT, all["SysOnly"])
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, all["PredOnly"])
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, all["CustomOnly"])
		// Predefined overwrites system.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, all["SysPred"])
		// Custom overwrites system.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, all["SysCustom"])
		// Custom overwrites predefined.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, all["PredCustom"])
		// Custom overwrites all.
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, all["AllThree"])
		r.Len(all, 7)
	})

	t.Run("EmptyMap", func(t *testing.T) {
		r := require.New(t)
		// Zero-value NameTypeMap falls back to global defaults (no custom).
		all := NameTypeMap{}.All()
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, all["RunId"])
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, all["TemporalChangeVersion"])
	})
}

func Test_WithPredefinedSearchAttributes(t *testing.T) {
	r := require.New(t)

	customSA := map[string]enumspb.IndexedValueType{
		"CustomKey": enumspb.INDEXED_VALUE_TYPE_TEXT,
	}
	base := NewNameTypeMap(customSA)

	// Baseline: default predefined includes TemporalChangeVersion from sadefs.Predefined().
	r.True(base.IsDefined("TemporalChangeVersion"))
	ivt, err := base.GetType("TemporalChangeVersion")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, ivt)

	// Custom attributes are preserved.
	ivt, err = base.GetType("CustomKey")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)

	// Override predefined with a custom set that does NOT include TemporalChangeVersion.
	overriddenPredefined := map[string]enumspb.IndexedValueType{
		"MyPredefined": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
	}
	overridden := base.WithPredefinedSearchAttributes(overriddenPredefined)

	// New predefined attribute is resolved.
	ivt, err = overridden.GetType("MyPredefined")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, ivt)

	// TemporalChangeVersion is no longer found via predefined (only system SAs remain).
	r.False(overridden.IsDefined("TemporalChangeVersion"))

	// Custom attributes are still preserved after override.
	ivt, err = overridden.GetType("CustomKey")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)

	// System search attributes (e.g. RunId) are still accessible.
	ivt, err = overridden.GetType("RunId")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)

	// Original base map is not mutated.
	r.True(base.IsDefined("TemporalChangeVersion"))
	r.False(base.IsDefined("MyPredefined"))

	// Chaining: override system first, then override predefined.
	// WithPredefinedSearchAttributes should preserve the system override.
	overriddenSystem := map[string]enumspb.IndexedValueType{
		"MySystem": enumspb.INDEXED_VALUE_TYPE_INT,
	}
	chained := base.WithSystemSearchAttributes(overriddenSystem).WithPredefinedSearchAttributes(overriddenPredefined)

	// Overridden system attribute is preserved through the chain.
	ivt, err = chained.GetType("MySystem")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_INT, ivt)

	// Default system SA (RunId) is no longer found since system was overridden.
	r.False(chained.IsDefined("RunId"))

	// Overridden predefined attribute is present.
	ivt, err = chained.GetType("MyPredefined")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, ivt)

	// Default predefined SA (TemporalChangeVersion) is no longer found since predefined was overridden.
	r.False(chained.IsDefined("TemporalChangeVersion"))

	// Custom attributes are still preserved.
	ivt, err = chained.GetType("CustomKey")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)
}

func Test_WithSystemSearchAttributes(t *testing.T) {
	r := require.New(t)

	customSA := map[string]enumspb.IndexedValueType{
		"CustomKey": enumspb.INDEXED_VALUE_TYPE_TEXT,
	}
	base := NewNameTypeMap(customSA)

	// Baseline: default system includes RunId from sadefs.System().
	r.True(base.IsDefined("RunId"))
	ivt, err := base.GetType("RunId")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)

	// Custom attributes are preserved.
	ivt, err = base.GetType("CustomKey")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)

	// Override system with a custom set that does NOT include RunId.
	overriddenSystem := map[string]enumspb.IndexedValueType{
		"MySystem": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
	}
	overridden := base.WithSystemSearchAttributes(overriddenSystem)

	// New system attribute is resolved.
	ivt, err = overridden.GetType("MySystem")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, ivt)

	// RunId is no longer found via system (only predefined and custom remain).
	r.False(overridden.IsDefined("RunId"))

	// Custom attributes are still preserved after override.
	ivt, err = overridden.GetType("CustomKey")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)

	// Predefined search attributes (e.g. TemporalChangeVersion) are still accessible.
	ivt, err = overridden.GetType("TemporalChangeVersion")
	r.NoError(err)
	r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, ivt)

	// Original base map is not mutated.
	r.True(base.IsDefined("RunId"))
	r.False(base.IsDefined("MySystem"))
}

func Test_MergeNameTypeMaps(t *testing.T) {
	t.Run("DisjointMaps", func(t *testing.T) {
		r := require.New(t)
		a := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"SysA": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"PredA": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"CustomA": enumspb.INDEXED_VALUE_TYPE_INT,
			},
		}
		b := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"SysB": enumspb.INDEXED_VALUE_TYPE_TEXT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"PredB": enumspb.INDEXED_VALUE_TYPE_BOOL,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"CustomB": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
		}

		merged := MergeNameTypeMaps(a, b)

		ivt, err := merged.GetType("SysA")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)

		ivt, err = merged.GetType("SysB")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)

		ivt, err = merged.GetType("PredA")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)

		ivt, err = merged.GetType("PredB")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, ivt)

		ivt, err = merged.GetType("CustomA")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_INT, ivt)

		ivt, err = merged.GetType("CustomB")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, ivt)
	})

	t.Run("SecondOverwritesFirst", func(t *testing.T) {
		r := require.New(t)
		a := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"SharedSys": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"Shared": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"SharedCustom": enumspb.INDEXED_VALUE_TYPE_INT,
			},
		}
		b := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"SharedSys": enumspb.INDEXED_VALUE_TYPE_TEXT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"Shared": enumspb.INDEXED_VALUE_TYPE_BOOL,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"SharedCustom": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
		}

		merged := MergeNameTypeMaps(a, b)

		// b's values win on conflict.
		ivt, err := merged.GetType("SharedSys")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)

		ivt, err = merged.GetType("Shared")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, ivt)

		ivt, err = merged.GetType("SharedCustom")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, ivt)
	})

	t.Run("DoesNotMutateInputs", func(t *testing.T) {
		r := require.New(t)
		a := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"SysA": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"PredA": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"CustomA": enumspb.INDEXED_VALUE_TYPE_INT,
			},
		}
		b := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"SysB": enumspb.INDEXED_VALUE_TYPE_TEXT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"PredB": enumspb.INDEXED_VALUE_TYPE_BOOL,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"CustomB": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
		}

		_ = MergeNameTypeMaps(a, b)

		// a should not contain b's entries.
		r.Len(a.systemSearchAttributes, 1)
		r.Len(a.predefinedSearchAttributes, 1)
		r.Len(a.customSearchAttributes, 1)
		_, hasSysB := a.systemSearchAttributes["SysB"]
		r.False(hasSysB)
		_, hasPredB := a.predefinedSearchAttributes["PredB"]
		r.False(hasPredB)
		_, hasCustomB := a.customSearchAttributes["CustomB"]
		r.False(hasCustomB)
	})

	t.Run("EmptyFirstMap", func(t *testing.T) {
		r := require.New(t)

		b := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"SysB": enumspb.INDEXED_VALUE_TYPE_TEXT,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"PredB": enumspb.INDEXED_VALUE_TYPE_BOOL,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"CustomB": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			},
		}
		empty := NameTypeMap{}

		// Merge with empty first map preserves second.
		merged := MergeNameTypeMaps(empty, b)
		ivt, err := merged.GetType("SysB")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT, ivt)
		ivt, err = merged.GetType("PredB")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_BOOL, ivt)
		ivt, err = merged.GetType("CustomB")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_DOUBLE, ivt)
	})

	t.Run("EmptySecondMap", func(t *testing.T) {
		r := require.New(t)

		a := NameTypeMap{
			systemSearchAttributes: map[string]enumspb.IndexedValueType{
				"SysA": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			predefinedSearchAttributes: map[string]enumspb.IndexedValueType{
				"PredA": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			customSearchAttributes: map[string]enumspb.IndexedValueType{
				"CustomA": enumspb.INDEXED_VALUE_TYPE_INT,
			},
		}
		empty := NameTypeMap{}

		// Merge with empty second map preserves first.
		merged := MergeNameTypeMaps(a, empty)
		ivt, err := merged.GetType("SysA")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)
		ivt, err = merged.GetType("PredA")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, ivt)
		ivt, err = merged.GetType("CustomA")
		r.NoError(err)
		r.Equal(enumspb.INDEXED_VALUE_TYPE_INT, ivt)
	})

	t.Run("BothEmpty", func(t *testing.T) {
		r := require.New(t)
		empty := NameTypeMap{}
		merged := MergeNameTypeMaps(empty, empty)
		r.Empty(merged.systemSearchAttributes)
		r.Empty(merged.predefinedSearchAttributes)
		r.Empty(merged.customSearchAttributes)
	})
}
