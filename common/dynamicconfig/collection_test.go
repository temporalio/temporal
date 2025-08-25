package dynamicconfig_test

import (
	"errors"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
)

const (
	// dynamic config for tests
	unknownKey                                        = "unknownKey"
	testGetPropertyKey                                = "testGetPropertyKey"
	testCaseInsensitivePropertyKey                    = "testCaseInsensitivePropertyKey"
	testGetIntPropertyKey                             = "testGetIntPropertyKey"
	testGetFloat64PropertyKey                         = "testGetFloat64PropertyKey"
	testGetDurationPropertyKey                        = "testGetDurationPropertyKey"
	testGetBoolPropertyKey                            = "testGetBoolPropertyKey"
	testGetStringPropertyKey                          = "testGetStringPropertyKey"
	testGetMapPropertyKey                             = "testGetMapPropertyKey"
	testGetTypedPropertyKey                           = "testGetTypedPropertyKey"
	testGetIntPropertyFilteredByNamespaceKey          = "testGetIntPropertyFilteredByNamespaceKey"
	testGetDurationPropertyFilteredByNamespaceKey     = "testGetDurationPropertyFilteredByNamespaceKey"
	testGetIntPropertyFilteredByTaskQueueInfoKey      = "testGetIntPropertyFilteredByTaskQueueInfoKey"
	testGetDurationPropertyFilteredByTaskQueueInfoKey = "testGetDurationPropertyFilteredByTaskQueueInfoKey"
	testGetDurationPropertyFilteredByTaskTypeKey      = "testGetDurationPropertyFilteredByTaskTypeKey"
	testGetDurationPropertyStructuredDefaults         = "testGetDurationPropertyStructuredDefaults"
	testGetBoolPropertyFilteredByNamespaceIDKey       = "testGetBoolPropertyFilteredByNamespaceIDKey"
	testGetBoolPropertyFilteredByTaskQueueInfoKey     = "testGetBoolPropertyFilteredByTaskQueueInfoKey"
	testGetStringPropertyFilteredByNamespaceIDKey     = "testGetStringPropertyFilteredByNamespaceIDKey"
	testGetIntPropertyFilteredByDestinationKey        = "testGetIntPropertyFilteredByDestinationKey"
)

// Note: fileBasedClientSuite also heavily tests Collection, since some tests are easier with data
// provided from a file.
type collectionSuite struct {
	suite.Suite
	client *testSubscribableClient
	cln    *dynamicconfig.Collection
}

func TestCollectionSuite(t *testing.T) {
	s := new(collectionSuite)
	suite.Run(t, s)
}

func (s *collectionSuite) SetupTest() {
	dynamicconfig.ResetRegistryForTest()
	s.client = newTestSubscribableClient()
	logger := log.NewNoopLogger()
	s.cln = dynamicconfig.NewCollection(s.client, logger)
	s.cln.Start()
}

func (s *collectionSuite) TearDownTest() {
	s.cln.Stop()
}

func (s *collectionSuite) TestGetIntProperty() {
	setting := dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 10, "")
	value := setting.Get(s.cln)
	s.Equal(10, value())
	s.client.SetValue(testGetIntPropertyKey, 50)
	s.Equal(50, value())
	s.client.SetValue(testGetIntPropertyKey, uint32(50000))
	s.Equal(50000, value())
}

func (s *collectionSuite) TestGetIntPropertyFilteredByNamespace() {
	setting := dynamicconfig.NewNamespaceIntSetting(testGetIntPropertyFilteredByNamespaceKey, 10, "")
	namespace := "testNamespace"
	value := setting.Get(s.cln)
	s.Equal(10, value(namespace))
	s.client.SetValue(testGetIntPropertyFilteredByNamespaceKey, 50)
	s.Equal(50, value(namespace))
}

func (s *collectionSuite) TestGetStringPropertyFnFilteredByNamespace() {
	namespace := "testNamespace"
	value := dynamicconfig.DefaultEventEncoding.Get(s.cln)
	// copied default value, change this if it changes
	s.Equal(enumspb.ENCODING_TYPE_PROTO3.String(), value(namespace))
	s.client.SetValue(dynamicconfig.DefaultEventEncoding.Key().String(), "efg")
	s.Equal("efg", value(namespace))
}

func (s *collectionSuite) TestGetStringPropertyFnFilteredByNamespaceID() {
	setting := dynamicconfig.NewNamespaceIDStringSetting(testGetStringPropertyFilteredByNamespaceIDKey, "abc", "")
	namespaceID := namespace.ID("testNamespaceID")
	value := setting.Get(s.cln)
	s.Equal("abc", value(namespaceID))
	s.client.SetValue(testGetStringPropertyFilteredByNamespaceIDKey, "efg")
	s.Equal("efg", value(namespaceID))
}

func (s *collectionSuite) TestGetIntPropertyFilteredByTaskQueueInfo() {
	setting := dynamicconfig.NewTaskQueueIntSetting(testGetIntPropertyFilteredByTaskQueueInfoKey, 10, "")
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := setting.Get(s.cln)
	s.Equal(10, value(namespace, taskQueue, 0))
	s.client.SetValue(testGetIntPropertyFilteredByTaskQueueInfoKey, 50)
	s.Equal(50, value(namespace, taskQueue, 0))
}

func (s *collectionSuite) TestGetFloat64Property() {
	setting := dynamicconfig.NewGlobalFloatSetting(testGetFloat64PropertyKey, 0.1, "")
	value := setting.Get(s.cln)
	s.InEpsilon(0.1, value(), 1e-10)
	s.client.SetValue(testGetFloat64PropertyKey, 0.01)
	s.InEpsilon(0.01, value(), 1e-10)
	s.client.SetValue(testGetFloat64PropertyKey, int64(123456789))
	s.InEpsilon(float64(123456789), value(), 1e-10)
}

func (s *collectionSuite) TestGetBoolProperty() {
	setting := dynamicconfig.NewGlobalBoolSetting(testGetBoolPropertyKey, true, "")
	value := setting.Get(s.cln)
	s.Equal(true, value())
	s.client.SetValue(testGetBoolPropertyKey, false)
	s.Equal(false, value())
	s.client.SetValue(testGetBoolPropertyKey, "false")
	s.Equal(false, value())
}

func (s *collectionSuite) TestGetBoolPropertyFilteredByNamespaceID() {
	setting := dynamicconfig.NewNamespaceIDBoolSetting(testGetBoolPropertyFilteredByNamespaceIDKey, true, "")
	namespaceID := namespace.ID("testNamespaceID")
	value := setting.Get(s.cln)
	s.Equal(true, value(namespaceID))
	s.client.SetValue(testGetBoolPropertyFilteredByNamespaceIDKey, false)
	s.Equal(false, value(namespaceID))
}

func (s *collectionSuite) TestGetBoolPropertyFilteredByTaskQueueInfo() {
	setting := dynamicconfig.NewTaskQueueBoolSetting(testGetBoolPropertyFilteredByTaskQueueInfoKey, false, "")
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := setting.Get(s.cln)
	s.Equal(false, value(namespace, taskQueue, 0))
	s.client.SetValue(testGetBoolPropertyFilteredByTaskQueueInfoKey, true)
	s.Equal(true, value(namespace, taskQueue, 0))
}

func (s *collectionSuite) TestGetDurationProperty() {
	setting := dynamicconfig.NewGlobalDurationSetting(testGetDurationPropertyKey, 1*time.Second, "")
	value := setting.Get(s.cln)
	s.Equal(time.Second, value())
	s.client.SetValue(testGetDurationPropertyKey, time.Minute)
	s.Equal(time.Minute, value())
	s.client.SetValue(testGetDurationPropertyKey, 33)
	s.Equal(33*time.Second, value())
	s.client.SetValue(testGetDurationPropertyKey, int16(33))
	s.Equal(33*time.Second, value())
	s.client.SetValue(testGetDurationPropertyKey, "33")
	s.Equal(33*time.Second, value())
	s.client.SetValue(testGetDurationPropertyKey, "33h")
	s.Equal(33*time.Hour, value())
	s.client.SetValue(testGetDurationPropertyKey, float32(33.5))
	s.Equal(33*time.Second+500*time.Millisecond, value())
}

func (s *collectionSuite) TestGetDurationPropertyFilteredByNamespace() {
	setting := dynamicconfig.NewNamespaceDurationSetting(testGetDurationPropertyFilteredByNamespaceKey, time.Second, "")
	namespace := "testNamespace"
	value := setting.Get(s.cln)
	s.Equal(time.Second, value(namespace))
	s.client.SetValue(testGetDurationPropertyFilteredByNamespaceKey, time.Minute)
	s.Equal(time.Minute, value(namespace))
}

func (s *collectionSuite) TestGetDurationPropertyFilteredByTaskQueueInfo() {
	setting := dynamicconfig.NewTaskQueueDurationSetting(testGetDurationPropertyFilteredByTaskQueueInfoKey, time.Second, "")
	namespace := "testNamespace"
	taskQueue := "testTaskQueue"
	value := setting.Get(s.cln)
	s.Equal(time.Second, value(namespace, taskQueue, 0))
	s.client.SetValue(testGetDurationPropertyFilteredByTaskQueueInfoKey, time.Minute)
	s.Equal(time.Minute, value(namespace, taskQueue, 0))
}

func (s *collectionSuite) TestGetDurationPropertyFilteredByTaskType() {
	setting := dynamicconfig.NewTaskTypeDurationSetting(testGetDurationPropertyFilteredByTaskTypeKey, time.Second, "")
	taskType := enumsspb.TASK_TYPE_UNSPECIFIED
	value := setting.Get(s.cln)
	s.Equal(time.Second, value(taskType))
	s.client.SetValue(testGetDurationPropertyFilteredByTaskTypeKey, time.Minute)
	s.Equal(time.Minute, value(taskType))
}

func (s *collectionSuite) TestGetDurationPropertyStructuredDefaults() {
	setting := dynamicconfig.NewTaskQueueDurationSettingWithConstrainedDefault(
		testGetDurationPropertyStructuredDefaults,
		[]dynamicconfig.TypedConstrainedValue[time.Duration]{
			{
				Constraints: dynamicconfig.Constraints{
					Namespace:     "ns2",
					TaskQueueName: "tq2",
				},
				Value: 2 * time.Minute,
			},
			{
				Constraints: dynamicconfig.Constraints{
					TaskQueueName: "tq2",
				},
				Value: 5 * time.Minute,
			},
			{
				Value: 7 * time.Minute,
			},
		},
		"",
	)
	value := setting.Get(s.cln)
	s.Equal(7*time.Minute, value("ns1", "tq1", 0))
	s.Equal(7*time.Minute, value("ns2", "tq1", 0))
	s.Equal(5*time.Minute, value("ns1", "tq2", 0))
	s.Equal(2*time.Minute, value("ns2", "tq2", 0))

	// user-set values should take precedence. defaults are included below in the interleaved
	// precedence order to make the test easier to read
	s.client.Set(testGetDurationPropertyStructuredDefaults, []dynamicconfig.ConstrainedValue{
		{
			Constraints: dynamicconfig.Constraints{
				Namespace:     "ns2",
				TaskQueueName: "tq2",
			},
			Value: 2 * time.Second,
		},
		// {
		//   Constraints: Constraints{
		//     Namespace:     "ns2",
		//     TaskQueueName: "tq2",
		//   },
		//   Value: 2 * time.Minute,
		// },
		// {
		//   Constraints: Constraints{
		//     TaskQueueName: "tq2",
		//   },
		//   Value: 5 * time.Minute,
		// },
		{
			Constraints: dynamicconfig.Constraints{
				Namespace: "ns1",
			},
			Value: 5 * time.Second,
		},
		{
			Value: 7 * time.Second,
		},
		// {
		//   Value: 7 * time.Minute,
		// },
	})

	s.Equal(5*time.Second, value("ns1", "tq1", 0))
	s.Equal(7*time.Second, value("ns2", "tq1", 0))
	s.Equal(5*time.Minute, value("ns1", "tq2", 0))
	s.Equal(2*time.Second, value("ns2", "tq2", 0))
}

func (s *collectionSuite) TestGetMapProperty() {
	def := map[string]interface{}{"testKey": 123}
	setting := dynamicconfig.NewGlobalMapSetting(
		testGetMapPropertyKey,
		def,
		"",
	)
	value := setting.Get(s.cln)
	s.Equal(def, value())
	val := maps.Clone(def)
	val["testKey"] = "321"
	s.client.SetValue(testGetMapPropertyKey, val)
	s.Equal(val, value())
	s.Equal("321", value()["testKey"])
}

func (s *collectionSuite) TestGetTyped() {
	type myFancyType struct {
		Number int
		Names  []string
	}
	def := myFancyType{28, []string{"global", "typed", "setting"}}
	setting := dynamicconfig.NewGlobalTypedSettingWithConverter(
		testGetTypedPropertyKey,
		dynamicconfig.ConvertStructure(myFancyType{-3, nil}), // used if convert is called
		def,
		"",
	)
	get := setting.Get(s.cln)

	s.Run("Default", func() {
		s.Equal(def, get())
	})

	s.Run("Basic", func() {
		// map[string]any is what the yaml library decodes arbitrary data into
		s.client.SetValue(testGetTypedPropertyKey, map[string]any{
			"Number": 39,
			"Names":  []string{"new", "names"},
		})
		s.Equal(myFancyType{
			Number: 39,
			Names:  []string{"new", "names"},
		}, get())
	})

	s.Run("CaseInsensitive", func() {
		s.client.SetValue(testGetTypedPropertyKey, map[string]any{
			"naMES": []string{"case", "insensitive"},
		})
		s.Equal(-3, get().Number) // note the convert default is used here
		s.Equal([]string{"case", "insensitive"}, get().Names)
	})

	s.Run("WrongType", func() {
		s.client.SetValue(testGetTypedPropertyKey, 200)
		s.Equal(def, get())
	})
}

func (s *collectionSuite) TestGetTypedSimpleList() {
	def := []float64{1.5, 1.1, 2.6, 3.7, 6.3}
	setting := dynamicconfig.NewGlobalTypedSettingWithConverter(
		testGetTypedPropertyKey,
		dynamicconfig.ConvertStructure([]float64(nil)),
		def,
		"",
	)
	get := setting.Get(s.cln)

	s.Run("Default", func() {
		s.Equal(def, get())
	})

	s.Run("Basic", func() {
		s.client.SetValue(testGetTypedPropertyKey, []any{19.0, -2.0})
		s.Equal([]float64{19.0, -2.0}, get())
	})

	s.Run("WrongType", func() {
		s.client.SetValue(testGetTypedPropertyKey, []any{88.8, false, -5, "oops"})
		s.Equal(def, get())
	})
}

func (s *collectionSuite) TestGetTypedListOfStruct() {
	type simple struct{ A, B int }
	def := []simple{{1, 5}, {2, 9}}
	setting := dynamicconfig.NewGlobalTypedSettingWithConverter(
		testGetTypedPropertyKey,
		dynamicconfig.ConvertStructure([]simple(nil)),
		def,
		"",
	)
	get := setting.Get(s.cln)

	s.Run("Default", func() {
		s.Equal(def, get())
	})

	s.Run("Basic", func() {
		s.client.SetValue(testGetTypedPropertyKey, []any{
			map[string]any{"A": 12, "B": 6},
			map[string]any{"A": -23, "B": 0},
			map[string]any{"B": 555, "C": "ignored"},
		})
		s.Equal([]simple{{12, 6}, {-23, 0}, {0, 555}}, get())
	})

	s.Run("WrongType", func() {
		s.client.SetValue(testGetTypedPropertyKey, []any{
			map[string]any{"A": false, "B": true},
		})
		s.Equal(def, get())
	})
}

func (s *collectionSuite) TestGetTypedProtoEnum() {
	def := enumspb.ARCHIVAL_STATE_UNSPECIFIED
	setting := dynamicconfig.NewGlobalTypedSetting(
		testGetTypedPropertyKey,
		def,
		"",
	)
	get := setting.Get(s.cln)

	s.Run("Default", func() {
		s.Equal(def, get())
	})

	s.Run("Basic", func() {
		s.client.SetValue(testGetTypedPropertyKey, "ARCHIVAL_STATE_DISABLED")
		s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, get())
	})

	s.Run("CaseInsensitive", func() {
		s.client.SetValue(testGetTypedPropertyKey, "archival_state_disabled")
		s.Equal(enumspb.ARCHIVAL_STATE_DISABLED, get())
	})

	s.Run("NotFound", func() {
		s.client.SetValue(testGetTypedPropertyKey, "some_other_string")
		s.Equal(def, get())
	})

	s.Run("Int", func() {
		s.client.SetValue(testGetTypedPropertyKey, 2)
		s.Equal(enumspb.ARCHIVAL_STATE_ENABLED, get())
	})

	s.Run("WrongType", func() {
		s.client.SetValue(testGetTypedPropertyKey, true)
		s.Equal(def, get())
	})
}

// someEnum is an example type for DynamicConfigParseHook.
type someEnum int32

const (
	someEnumValueUnset someEnum = iota
	someEnumValueOne
	someEnumValueTwo
	someEnumValueThree
)

func (someEnum) DynamicConfigParseHook(s string) (someEnum, error) {
	switch strings.ToLower(s) {
	case "one":
		return someEnumValueOne, nil
	case "two":
		return someEnumValueTwo, nil
	case "three":
		return someEnumValueThree, nil
	default:
		return 0, errors.New("unknown value")
	}
}

func (s *collectionSuite) TestGetGenericParseHook() {
	def := someEnumValueOne
	setting := dynamicconfig.NewGlobalTypedSetting(
		testGetTypedPropertyKey,
		def,
		"",
	)
	get := setting.Get(s.cln)

	s.Run("Default", func() {
		s.Equal(def, get())
	})

	s.Run("Basic", func() {
		s.client.SetValue(testGetTypedPropertyKey, "THRee")
		s.Equal(someEnumValueThree, get())
	})

	s.Run("Missing", func() {
		s.client.SetValue(testGetTypedPropertyKey, "four")
		s.Equal(def, get()) // default since there was a parse error
	})
}

func (s *collectionSuite) TestGetGenericParseHookValue_Struct() {
	type myStruct struct {
		FieldA someEnum
		FieldB someEnum
	}
	def := myStruct{
		FieldA: someEnumValueTwo,
		FieldB: someEnumValueThree,
	}
	setting := dynamicconfig.NewGlobalTypedSetting(
		testGetTypedPropertyKey,
		def,
		"",
	)
	get := setting.Get(s.cln)

	s.Run("Default", func() {
		s.Equal(def, get())
	})

	s.Run("Basic", func() {
		s.client.SetValue(testGetTypedPropertyKey, map[string]any{"fielda": "one"})
		s.Equal(myStruct{
			FieldA: someEnumValueOne,
			FieldB: someEnumValueThree, // from default
		}, get())
	})

	s.Run("Missing", func() {
		s.client.SetValue(testGetTypedPropertyKey, map[string]any{"FieldA": "one", "FieldB": "four"})
		s.Equal(def, get()) // default since there was a parse error
	})
}

func (s *collectionSuite) TestGetIntPropertyFilteredByDestination() {
	setting := dynamicconfig.NewDestinationIntSetting(testGetIntPropertyFilteredByDestinationKey, 10, "")
	namespaceName := "testNamespace"
	destination1 := "testDestination1"
	destination2 := "testDestination2"
	value := setting.Get(s.cln)
	s.Equal(10, value(namespaceName, destination1))
	s.client.Set(testGetIntPropertyFilteredByDestinationKey, []dynamicconfig.ConstrainedValue{
		{
			Constraints: dynamicconfig.Constraints{
				Namespace:   namespaceName,
				Destination: destination1,
			},
			Value: 50,
		},
		{
			Constraints: dynamicconfig.Constraints{
				Namespace: namespaceName,
			},
			Value: 75,
		},
		{
			Constraints: dynamicconfig.Constraints{
				Destination: destination1,
			},
			Value: 90,
		},
		{
			Constraints: dynamicconfig.Constraints{
				Destination: destination2,
			},
			Value: 100,
		},
	})
	s.Equal(50, value(namespaceName, destination1))
	s.Equal(75, value(namespaceName, "testAnotherDestination"))
	s.Equal(90, value("testAnotherNamespace", destination1))
	s.Equal(100, value(namespaceName, destination2)) // priority: destination >>> namespace
	s.Equal(10, value("testAnotherNamespace", "testAnotherDestination"))
}

type (
	subscriptionSuite struct {
		suite.Suite
		client *testSubscribableClient
		cln    *dynamicconfig.Collection
	}

	testSubscribableClient struct {
		lock sync.Mutex
		m    map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue
		subs []dynamicconfig.ClientUpdateFunc
	}
)

var _ dynamicconfig.NotifyingClient = (*testSubscribableClient)(nil)

func TestSubscriptionSuite(t *testing.T) {
	suite.Run(t, new(subscriptionSuite))
}

func (s *subscriptionSuite) SetupSuite() {
	s.client = newTestSubscribableClient()
	logger := log.NewNoopLogger()
	s.cln = dynamicconfig.NewCollection(s.client, logger)
	s.cln.Start()
}

func (s *subscriptionSuite) TearDownSuite() {
	s.cln.Stop()
}

func (s *subscriptionSuite) SetupTest() {
	dynamicconfig.ResetRegistryForTest()
}

func newTestSubscribableClient() *testSubscribableClient {
	return &testSubscribableClient{
		m: make(map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue),
	}
}

func (c *testSubscribableClient) Subscribe(f dynamicconfig.ClientUpdateFunc) func() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.subs = append(c.subs, f)
	return func() {} // ignore cancel
}

func (c *testSubscribableClient) GetValue(k dynamicconfig.Key) []dynamicconfig.ConstrainedValue {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.m[k]
}

func (c *testSubscribableClient) SetValue(k string, v any) {
	c.Set(k, []dynamicconfig.ConstrainedValue{{Value: v}})
}

func (c *testSubscribableClient) Set(ks string, cvs []dynamicconfig.ConstrainedValue) {
	k := dynamicconfig.MakeKey(ks)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.m[k] = cvs
	for _, f := range c.subs {
		f(map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue{k: cvs})
	}
}

func (s *subscriptionSuite) TestSubscriptionGlobal() {
	setting := dynamicconfig.NewGlobalBoolSetting(testGetBoolPropertyKey, false, "")

	vals := make(chan bool, 1)
	cb := func(newVal bool) { vals <- newVal }
	initial, cancel := setting.Subscribe(s.cln)(cb)

	s.False(initial)

	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{{Value: true}})
	s.Require().Eventually(func() bool { return len(vals) == 1 }, time.Second, time.Millisecond)
	s.True(<-vals)

	s.client.Set(setting.Key().String(), nil) // back to default
	s.Require().Eventually(func() bool { return len(vals) == 1 }, time.Second, time.Millisecond)
	s.False(<-vals)

	cancel()

	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{{Value: true}})
	// no update should be delivered
	time.Sleep(10 * time.Millisecond)
	s.Empty(vals, "should not deliver update")
}

func (s *subscriptionSuite) TestSubscriptionGlobal_DoesNotCallUnchanged() {
	setting := dynamicconfig.NewGlobalBoolSetting(testGetBoolPropertyKey, true, "")
	vals := make(chan bool, 1)
	cb := func(newVal bool) { vals <- newVal }
	initial, _ := setting.Subscribe(s.cln)(cb)
	s.True(initial)
	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{{Value: true}})
	time.Sleep(10 * time.Millisecond)
	s.Empty(vals, "should not deliver update")
}

func (s *subscriptionSuite) TestSubscriptionNamespace() {
	setting := dynamicconfig.NewNamespaceIntSetting(testGetIntPropertyKey, 0, "")

	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{Namespace: "ns1"}, Value: 1},
		{Constraints: dynamicconfig.Constraints{Namespace: "ns3"}, Value: 3},
	})

	vals1 := make(chan int, 1)
	init1, _ := setting.Subscribe(s.cln)("ns1", func(n int) { vals1 <- n })
	vals2 := make(chan int, 1)
	init2, _ := setting.Subscribe(s.cln)("ns2", func(n int) { vals2 <- n })
	vals3 := make(chan int, 1)
	init3, _ := setting.Subscribe(s.cln)("ns3", func(n int) { vals3 <- n })

	s.Equal(1, init1)
	s.Equal(0, init2)
	s.Equal(3, init3)

	// change ns3 to 33
	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{Namespace: "ns1"}, Value: 1},
		{Constraints: dynamicconfig.Constraints{Namespace: "ns3"}, Value: 33},
	})

	s.Require().Eventually(func() bool { return len(vals3) == 1 }, time.Second, time.Millisecond)
	s.Equal(33, <-vals3)
	s.Empty(vals1)
	s.Empty(vals2)

	// add ns2
	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{Namespace: "ns1"}, Value: 1},
		{Constraints: dynamicconfig.Constraints{Namespace: "ns2"}, Value: 2},
		{Constraints: dynamicconfig.Constraints{Namespace: "ns3"}, Value: 33},
	})
	s.Require().Eventually(func() bool { return len(vals2) == 1 }, time.Second, time.Millisecond)
	s.Equal(2, <-vals2)
	s.Empty(vals1)
	s.Empty(vals3)

	// remove ns1 and ns3
	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{Namespace: "ns2"}, Value: 2},
	})
	s.Require().Eventually(
		func() bool { return len(vals1) == 1 && len(vals3) == 1 },
		time.Second, time.Millisecond)
	s.Equal(0, <-vals1)
	s.Empty(vals2)
	s.Equal(0, <-vals3)
}

func (s *subscriptionSuite) TestSubscriptionWithDefault() {
	baseSetting := dynamicconfig.NewGlobalIntSetting(testGetIntPropertyKey, 0, "")
	setting := baseSetting.WithDefault(100)

	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{{Value: 50}})

	vals := make(chan int, 1)
	init, _ := setting.Subscribe(s.cln)(func(n int) { vals <- n })
	s.Equal(50, init)

	// remove, should get default
	s.client.Set(setting.Key().String(), nil)
	s.Require().Eventually(func() bool { return len(vals) == 1 }, time.Second, time.Millisecond)
	s.Equal(100, <-vals)

	// test nil callback
	v, cancel := setting.Subscribe(s.cln)(nil)
	s.Equal(100, v)
	s.Nil(cancel)
}

func (s *subscriptionSuite) TestSubscriptionConstrainedDefaults() {
	setting := dynamicconfig.NewNamespaceIntSettingWithConstrainedDefault(
		testGetIntPropertyKey,
		[]dynamicconfig.TypedConstrainedValue[int]{
			{Value: 34, Constraints: dynamicconfig.Constraints{Namespace: "special"}},
			{Value: 10}, // no constraints = default for all
		},
		"",
	)

	var normal, special atomic.Int64
	var normalCalls, specialCalls atomic.Int64

	waitFor := func(normalv, specialv, normalc, specialc int) {
		s.EventuallyWithT(func(c *assert.CollectT) {
			assert.Equal(c, normalv, int(normal.Load()))
			assert.Equal(c, specialv, int(special.Load()))
		}, time.Second, time.Millisecond)
		s.Equal(normalc, int(normalCalls.Load()))
		s.Equal(specialc, int(specialCalls.Load()))
	}

	// normal ns
	normalInit, normalCancel := setting.Subscribe(s.cln)("normal", func(v int) { normal.Store(int64(v)); normalCalls.Add(1) })
	normal.Store(int64(normalInit))
	defer normalCancel()
	s.Equal(10, normalInit)

	// special ns
	specialInit, specialCancel := setting.Subscribe(s.cln)("special", func(v int) { special.Store(int64(v)); specialCalls.Add(1) })
	special.Store(int64(specialInit))
	defer specialCancel()
	s.Equal(34, specialInit) // Should get the constrained default for "special"

	// set a value for special
	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{
		{Value: 200, Constraints: dynamicconfig.Constraints{Namespace: "special"}},
	})
	waitFor(10, 200, 0, 1)

	// set a value for normal
	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{
		{Value: 123, Constraints: dynamicconfig.Constraints{Namespace: "normal"}},
	})
	waitFor(123, 34, 1, 2)

	// set a default value
	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{
		{Value: 19},
	})
	waitFor(19, 34, 2, 2)

	// remove values
	s.client.Set(setting.Key().String(), []dynamicconfig.ConstrainedValue{})
	waitFor(10, 34, 3, 2)
}
