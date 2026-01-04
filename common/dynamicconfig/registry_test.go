package dynamicconfig_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

func TestListSettings(t *testing.T) {
	dynamicconfig.ResetRegistryForTest()

	// Register some test settings
	setting1 := dynamicconfig.NewGlobalBoolSetting("test.setting1", true, "Test setting 1 description")
	setting2 := dynamicconfig.NewGlobalIntSetting("test.setting2", 42, "Test setting 2 description")
	setting3 := dynamicconfig.NewNamespaceStringSetting("test.setting3", "default", "Test setting 3 description")

	// List all settings
	settings := dynamicconfig.ListSettings()

	// Verify we got at least our 3 settings
	require.GreaterOrEqual(t, len(settings), 3)

	// Build a map for easier lookup
	settingsByKey := make(map[string]dynamicconfig.GenericSetting)
	for _, s := range settings {
		settingsByKey[s.Key().String()] = s
	}

	// Verify setting1
	s1, ok := settingsByKey["test.setting1"]
	require.True(t, ok, "setting1 should be in the list")
	assert.Equal(t, setting1.Key(), s1.Key())
	assert.Equal(t, dynamicconfig.PrecedenceGlobal, s1.Precedence())
	assert.Equal(t, "Test setting 1 description", s1.Description())
	assert.Equal(t, true, s1.DefaultValue())

	// Verify setting2
	s2, ok := settingsByKey["test.setting2"]
	require.True(t, ok, "setting2 should be in the list")
	assert.Equal(t, setting2.Key(), s2.Key())
	assert.Equal(t, dynamicconfig.PrecedenceGlobal, s2.Precedence())
	assert.Equal(t, "Test setting 2 description", s2.Description())
	assert.Equal(t, 42, s2.DefaultValue())

	// Verify setting3 (Namespace precedence)
	s3, ok := settingsByKey["test.setting3"]
	require.True(t, ok, "setting3 should be in the list")
	assert.Equal(t, setting3.Key(), s3.Key())
	assert.Equal(t, dynamicconfig.PrecedenceNamespace, s3.Precedence())
	assert.Equal(t, "Test setting 3 description", s3.Description())
	assert.Equal(t, "default", s3.DefaultValue())
}

func TestSettingDescriptionAndDefaultValue(t *testing.T) {
	dynamicconfig.ResetRegistryForTest()

	tests := []struct {
		name        string
		setting     dynamicconfig.GenericSetting
		wantDesc    string
		wantDefault any
		wantPrec    dynamicconfig.Precedence
	}{
		{
			name:        "GlobalBool",
			setting:     dynamicconfig.NewGlobalBoolSetting("test.bool", false, "A boolean setting"),
			wantDesc:    "A boolean setting",
			wantDefault: false,
			wantPrec:    dynamicconfig.PrecedenceGlobal,
		},
		{
			name:        "GlobalInt",
			setting:     dynamicconfig.NewGlobalIntSetting("test.int", 100, "An integer setting"),
			wantDesc:    "An integer setting",
			wantDefault: 100,
			wantPrec:    dynamicconfig.PrecedenceGlobal,
		},
		{
			name:        "GlobalFloat",
			setting:     dynamicconfig.NewGlobalFloatSetting("test.float", 3.14, "A float setting"),
			wantDesc:    "A float setting",
			wantDefault: 3.14,
			wantPrec:    dynamicconfig.PrecedenceGlobal,
		},
		{
			name:        "GlobalString",
			setting:     dynamicconfig.NewGlobalStringSetting("test.string", "hello", "A string setting"),
			wantDesc:    "A string setting",
			wantDefault: "hello",
			wantPrec:    dynamicconfig.PrecedenceGlobal,
		},
		{
			name:        "GlobalDuration",
			setting:     dynamicconfig.NewGlobalDurationSetting("test.duration", time.Minute, "A duration setting"),
			wantDesc:    "A duration setting",
			wantDefault: time.Minute,
			wantPrec:    dynamicconfig.PrecedenceGlobal,
		},
		{
			name:        "GlobalMap",
			setting:     dynamicconfig.NewGlobalMapSetting("test.map", map[string]any{"key": "value"}, "A map setting"),
			wantDesc:    "A map setting",
			wantDefault: map[string]any{"key": "value"},
			wantPrec:    dynamicconfig.PrecedenceGlobal,
		},
		{
			name:        "NamespaceBool",
			setting:     dynamicconfig.NewNamespaceBoolSetting("test.ns.bool", true, "Namespace bool"),
			wantDesc:    "Namespace bool",
			wantDefault: true,
			wantPrec:    dynamicconfig.PrecedenceNamespace,
		},
		{
			name:        "TaskQueueInt",
			setting:     dynamicconfig.NewTaskQueueIntSetting("test.tq.int", 50, "Task queue int"),
			wantDesc:    "Task queue int",
			wantDefault: 50,
			wantPrec:    dynamicconfig.PrecedenceTaskQueue,
		},
		{
			name:        "ShardIDDuration",
			setting:     dynamicconfig.NewShardIDDurationSetting("test.shard.duration", time.Second, "Shard duration"),
			wantDesc:    "Shard duration",
			wantDefault: time.Second,
			wantPrec:    dynamicconfig.PrecedenceShardID,
		},
		{
			name:        "NamespaceIDFloat",
			setting:     dynamicconfig.NewNamespaceIDFloatSetting("test.nsid.float", 2.71, "Namespace ID float"),
			wantDesc:    "Namespace ID float",
			wantDefault: 2.71,
			wantPrec:    dynamicconfig.PrecedenceNamespaceID,
		},
		{
			name:        "TaskTypeString",
			setting:     dynamicconfig.NewTaskTypeStringSetting("test.tasktype.string", "default-task", "Task type string"),
			wantDesc:    "Task type string",
			wantDefault: "default-task",
			wantPrec:    dynamicconfig.PrecedenceTaskType,
		},
		{
			name:        "DestinationBool",
			setting:     dynamicconfig.NewDestinationBoolSetting("test.dest.bool", true, "Destination bool"),
			wantDesc:    "Destination bool",
			wantDefault: true,
			wantPrec:    dynamicconfig.PrecedenceDestination,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantDesc, tt.setting.Description())
			assert.Equal(t, tt.wantDefault, tt.setting.DefaultValue())
			assert.Equal(t, tt.wantPrec, tt.setting.Precedence())
		})
	}
}

func TestConstrainedDefaultSettingReturnsNilDefaultValue(t *testing.T) {
	dynamicconfig.ResetRegistryForTest()

	// Create a setting with constrained defaults
	cdef := []dynamicconfig.TypedConstrainedValue[int]{
		{
			Constraints: dynamicconfig.Constraints{Namespace: "ns1"},
			Value:       100,
		},
		{
			Constraints: dynamicconfig.Constraints{},
			Value:       50,
		},
	}

	setting := dynamicconfig.NewNamespaceIntSettingWithConstrainedDefault(
		"test.constrained",
		cdef,
		"A setting with constrained defaults",
	)

	// DefaultValue should return nil for constrained default settings
	assert.Nil(t, setting.DefaultValue())
	assert.Equal(t, "A setting with constrained defaults", setting.Description())
	assert.Equal(t, dynamicconfig.PrecedenceNamespace, setting.Precedence())
}

func TestGetConfiguredValues(t *testing.T) {
	dynamicconfig.ResetRegistryForTest()

	// Create a test client and collection
	client := dynamicconfig.NewMemoryClient()
	logger := log.NewNoopLogger()
	cln := dynamicconfig.NewCollection(client, logger)

	// Register a setting
	setting := dynamicconfig.NewNamespaceIntSetting("test.config.value", 10, "Test setting")

	// Initially, there should be no configured values
	values := cln.GetConfiguredValues(setting.Key())
	assert.Empty(t, values)

	// Set some configured values using []ConstrainedValue
	configuredValues := []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{}, Value: 100},
		{Constraints: dynamicconfig.Constraints{Namespace: "ns1"}, Value: 200},
	}
	client.OverrideValue(setting.Key(), configuredValues)

	// Now GetConfiguredValues should return the configured overrides
	values = cln.GetConfiguredValues(setting.Key())
	assert.Len(t, values, 2)

	// Verify values (order might vary)
	foundGlobal := false
	foundNamespace := false
	for _, cv := range values {
		if cv.Constraints.Namespace == "" {
			assert.Equal(t, 100, cv.Value)
			foundGlobal = true
		}
		if cv.Constraints.Namespace == "ns1" {
			assert.Equal(t, 200, cv.Value)
			foundNamespace = true
		}
	}
	assert.True(t, foundGlobal, "should have global override")
	assert.True(t, foundNamespace, "should have namespace override")
}
