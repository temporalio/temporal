package dynamicconfig

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	"github.com/mitchellh/mapstructure"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/pingable"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type (
	// Collection implements lookup and constraint logic on top of a Client.
	// The rest of the server code should use Collection as the interface to dynamic config,
	// instead of the low-level Client.
	Collection struct {
		client   Client
		logger   log.Logger
		errCount int64

		cancelClientSubscription func()

		subscriptionLock sync.Mutex          // protects subscriptions, subscriptionIdx, and callbackPool
		subscriptions    map[Key]map[int]any // final "any" is *subscription[T]
		subscriptionIdx  int
		callbackPool     *goro.AdaptivePool

		poller goro.Group

		// cache converted values. use weak pointers to avoid holding on to values in the cache
		// that are no longer in use.
		convertCache sync.Map // map[weak.Pointer[ConstrainedValue]]any
	}

	subscription[T any] struct {
		// constant:
		prec []Constraints
		f    func(T)
		def  T
		cdef []TypedConstrainedValue[T] // nil for regular settings, populated for constrained default settings
		// protected by subscriptionLock in Collection:
		raw any // raw value that last sent value was converted from
	}

	subscriptionCallbackSettings struct {
		MinWorkers   int
		MaxWorkers   int
		TargetDelay  time.Duration
		ShrinkFactor float64
	}

	// sentinel type that doesn't compare equal to anything else
	defaultValue struct{}

	// These function types follow a similar pattern:
	//   {X}PropertyFn - returns a value of type X that is global (no filters)
	//   {X}PropertyFnWith{Y}Filter - returns a value of type X with the given filters
	// Available value types:
	//   Bool: bool
	//   Duration: time.Duration
	//   Float: float64
	//   Int: int
	//   Map: map[string]any
	//   String: string
	// Available filters:
	//   Namespace func(namespace string)
	//   NamespaceID func(namespaceID string)
	//   TaskQueue func(namespace string, taskQueue string, taskType enumspb.TaskQueueType)  (matching task queue)
	//   TaskType func(taskType enumspsb.TaskType)  (history task type)
	//   ShardID func(shardID int32)
)

const (
	errCountLogThreshold = 1000
)

var (
	errKeyNotPresent        = errors.New("key not present")
	errNoMatchingConstraint = errors.New("no matching constraint in key")

	protoEnumType = reflect.TypeOf((*protoreflect.Enum)(nil)).Elem()
	errorType     = reflect.TypeOf((*error)(nil)).Elem()
	durationType  = reflect.TypeOf(time.Duration(0))
	stringType    = reflect.TypeOf("")

	usingDefaultValue any = defaultValue{}
)

// NewCollection creates a new collection. For subscriptions to work, you must call Start/Stop.
// Get will work without Start/Stop.
func NewCollection(client Client, logger log.Logger) *Collection {
	return &Collection{
		client:        client,
		logger:        logger,
		errCount:      -1,
		subscriptions: make(map[Key]map[int]any),
	}
}

func (c *Collection) Start() {
	s := DynamicConfigSubscriptionCallback.Get(c)()
	c.subscriptionLock.Lock()
	defer c.subscriptionLock.Unlock()
	c.callbackPool = goro.NewAdaptivePool(clock.NewRealTimeSource(), s.MinWorkers, s.MaxWorkers, s.TargetDelay, s.ShrinkFactor)
	if notifyingClient, ok := c.client.(NotifyingClient); ok {
		c.cancelClientSubscription = notifyingClient.Subscribe(c.keysChanged)
	} else {
		c.poller.Go(c.pollForChanges)
	}
}

func (c *Collection) Stop() {
	c.poller.Cancel()
	c.poller.Wait()
	if c.cancelClientSubscription != nil {
		c.cancelClientSubscription()
	}
	c.subscriptionLock.Lock()
	defer c.subscriptionLock.Unlock()
	c.callbackPool.Stop()
	c.callbackPool = nil
}

// Implement pingable.Pingable
func (c *Collection) GetPingChecks() []pingable.Check {
	return []pingable.Check{
		{
			Name:    "dynamic config callbacks",
			Timeout: 5 * time.Second,
			Ping: func() []pingable.Pingable {
				c.subscriptionLock.Lock()
				defer c.subscriptionLock.Unlock()
				if c.callbackPool == nil {
					return nil
				}
				var wg sync.WaitGroup
				wg.Add(1)
				c.callbackPool.Do(wg.Done)
				wg.Wait()
				return nil
			},
		},
	}
}

func (c *Collection) pollForChanges(ctx context.Context) error {
	interval := DynamicConfigSubscriptionPollInterval.Get(c)
	for ctx.Err() == nil {
		util.InterruptibleSleep(ctx, interval())
		c.pollOnce()
	}
	return ctx.Err()
}

func (c *Collection) pollOnce() {
	c.subscriptionLock.Lock()
	defer c.subscriptionLock.Unlock()
	if c.callbackPool == nil {
		return
	}

	for key, subs := range c.subscriptions {
		setting := queryRegistry(key)
		if setting == nil {
			continue
		}
		for _, sub := range subs {
			cvs := c.client.GetValue(key)
			setting.dispatchUpdate(c, sub, cvs)
		}
	}
}

func (c *Collection) keysChanged(changed map[Key][]ConstrainedValue) {
	c.subscriptionLock.Lock()
	defer c.subscriptionLock.Unlock()
	if c.callbackPool == nil {
		return
	}

	for key, cvs := range changed {
		setting := queryRegistry(key)
		if setting == nil {
			continue
		}
		// use setting.Key instead of key to avoid changing case again
		for _, sub := range c.subscriptions[setting.Key()] {
			setting.dispatchUpdate(c, sub, cvs)
		}
	}
}

func (c *Collection) throttleLog() bool {
	// TODO: This is a lot of unnecessary contention with little benefit. Consider using
	// https://github.com/cespare/percpu here.
	errCount := atomic.AddInt64(&c.errCount, 1)
	// log only the first x errors and then one every x after that to reduce log noise
	return errCount < errCountLogThreshold || errCount%errCountLogThreshold == 0
}

func findMatch(cvs []ConstrainedValue, precedence []Constraints) (*ConstrainedValue, error) {
	if len(cvs) == 0 {
		return nil, errKeyNotPresent
	}
	for _, m := range precedence {
		for idx, cv := range cvs {
			if m == cv.Constraints {
				// Note: cvs here is the slice returned by Client.GetValue. We want to return a
				// pointer into that slice so that the converted value is cached as long as the
				// Client keeps the []ConstrainedValue alive. See the comment on
				// Client.GetValue.
				return &cvs[idx], nil
			}
		}
	}
	// key is present but no constraint section matches
	return nil, errNoMatchingConstraint
}

// matchAndConvert can't be a method of Collection because methods can't be generic, but we can
// take a *Collection as an argument.
func matchAndConvert[T any](
	c *Collection,
	key Key,
	def T,
	convert func(value any) (T, error),
	precedence []Constraints,
) T {
	cvs := c.client.GetValue(key)
	v, _ := matchAndConvertCvs(c, key, def, convert, precedence, cvs)
	return v
}

func matchAndConvertCvs[T any](
	c *Collection,
	key Key,
	def T,
	convert func(value any) (T, error),
	precedence []Constraints,
	cvs []ConstrainedValue,
) (T, any) {
	cvp, err := findMatch(cvs, precedence)
	if err != nil {
		if c.throttleLog() {
			c.logger.Debug("No such key in dynamic config, using default", tag.Key(key.String()), tag.Error(err))
		}
		// couldn't find a constrained match, use default
		return def, usingDefaultValue
	}

	typedVal, err := convertWithCache(c, key, convert, cvp)
	if err != nil {
		// We failed to convert the value to the desired type. Use the default.
		if c.throttleLog() {
			c.logger.Warn("Failed to convert value, using default", tag.Key(key.String()), tag.IgnoredValue(cvp), tag.Error(err))
		}
		return def, usingDefaultValue
	}
	return typedVal, cvp.Value
}

// Returns matched value out of cvs, matched default out of defaultCVs, and also the priorities
// of each of the matches (lower matched first). For no match, order will be 0.
func findMatchWithConstrainedDefaults[T any](cvs []ConstrainedValue, defaultCVs []TypedConstrainedValue[T], precedence []Constraints) (
	matchedValue *ConstrainedValue,
	matchedDefault T,
	valueOrder int,
	defaultOrder int,
) {
	order := 0
	for _, m := range precedence {
		for idx, cv := range cvs {
			order++
			if m == cv.Constraints {
				if valueOrder == 0 {
					valueOrder = order
					// Note: cvs here is the slice returned by Client.GetValue. We want to
					// return a pointer into that slice instead of copying the ConstrainedValue.
					// See findMatch.
					matchedValue = &cvs[idx]
				}
			}
		}
		for _, cv := range defaultCVs {
			order++
			if m == cv.Constraints {
				if defaultOrder == 0 {
					defaultOrder = order
					matchedDefault = cv.Value
				}
			}
		}
	}
	return
}

func findAndResolveWithConstrainedDefaults[T any](
	c *Collection,
	key Key,
	convert func(value any) (T, error),
	cvs []ConstrainedValue,
	defaultCVs []TypedConstrainedValue[T],
	precedence []Constraints,
) (value T, raw any) {
	cvp, defVal, valOrder, defOrder := findMatchWithConstrainedDefaults(cvs, defaultCVs, precedence)

	if defOrder == 0 {
		// This is a server bug: all precedence lists must end with no-constraints, and all
		// constrained defaults must have a no-constraints value, so we should have gotten a match.
		c.logger.Warn("Constrained defaults had no match (this is a bug; fix server code)", tag.Key(key.String()))
		// leave value as the zero value, that's the best we can do
		return value, usingDefaultValue
	} else if valOrder == 0 {
		if c.throttleLog() {
			c.logger.Debug("No such key in dynamic config, using default", tag.Key(key.String()))
		}
		return defVal, usingDefaultValue
	} else if defOrder < valOrder {
		// value was present but constrained default took precedence
		return defVal, usingDefaultValue // use sentinel since we're using default
	}
	typedVal, err := convertWithCache(c, key, convert, cvp)
	if err != nil {
		// We failed to convert the value to the desired type. Use the default.
		if c.throttleLog() {
			c.logger.Warn("Failed to convert value, using default", tag.Key(key.String()), tag.IgnoredValue(cvp), tag.Error(err))
		}
		return defVal, usingDefaultValue
	}
	return typedVal, cvp.Value
}

func matchAndConvertWithConstrainedDefault[T any](
	c *Collection,
	key Key,
	cdef []TypedConstrainedValue[T],
	convert func(value any) (T, error),
	precedence []Constraints,
) T {
	cvs := c.client.GetValue(key)
	value, _ := findAndResolveWithConstrainedDefaults(c, key, convert, cvs, cdef, precedence)
	return value
}

func subscribe[T any](
	c *Collection,
	key Key,
	def T,
	convert func(value any) (T, error),
	prec []Constraints,
	callback func(T),
) (T, func()) {
	c.subscriptionLock.Lock()
	defer c.subscriptionLock.Unlock()

	// get one value immediately (note that subscriptionLock is held here so we can't race with
	// an update)
	cvs := c.client.GetValue(key)
	init, raw := matchAndConvertCvs(c, key, def, convert, prec, cvs)

	// As a convenience (and for efficiency), you can pass in a nil callback; we just return the
	// current value and skip the subscription.  The cancellation func returned is also nil.
	if callback == nil {
		return init, nil
	}

	c.subscriptionIdx++
	id := c.subscriptionIdx

	if c.subscriptions[key] == nil {
		c.subscriptions[key] = make(map[int]any)
	}

	c.subscriptions[key][id] = &subscription[T]{
		prec: prec,
		f:    callback,
		def:  def,
		raw:  raw,
	}

	return init, func() {
		c.subscriptionLock.Lock()
		defer c.subscriptionLock.Unlock()
		delete(c.subscriptions[key], id)
	}
}

func subscribeWithConstrainedDefault[T any](
	c *Collection,
	key Key,
	cdef []TypedConstrainedValue[T],
	convert func(value any) (T, error),
	prec []Constraints,
	callback func(T),
) (T, func()) {
	c.subscriptionLock.Lock()
	defer c.subscriptionLock.Unlock()

	// get one value immediately (note that subscriptionLock is held here so we can't race with
	// an update)
	cvs := c.client.GetValue(key)
	init, raw := findAndResolveWithConstrainedDefaults(c, key, convert, cvs, cdef, prec)

	// As a convenience (and for efficiency), you can pass in a nil callback; we just return the
	// current value and skip the subscription. The cancellation func returned is also nil.
	if callback == nil {
		return init, nil
	}

	c.subscriptionIdx++
	id := c.subscriptionIdx

	if c.subscriptions[key] == nil {
		c.subscriptions[key] = make(map[int]any)
	}

	c.subscriptions[key][id] = &subscription[T]{
		prec: prec,
		f:    callback,
		cdef: cdef,
		raw:  raw,
	}

	return init, func() {
		c.subscriptionLock.Lock()
		defer c.subscriptionLock.Unlock()
		delete(c.subscriptions[key], id)
	}
}

// called with subscriptionLock
func dispatchUpdate[T any](
	c *Collection,
	key Key,
	convert func(value any) (T, error),
	sub *subscription[T],
	cvs []ConstrainedValue,
) {
	var raw any
	cvp, err := findMatch(cvs, sub.prec)
	if err != nil {
		if c.throttleLog() {
			c.logger.Debug("No such key in dynamic config, using default", tag.Key(key.String()), tag.Error(err))
		}
		raw = usingDefaultValue
	} else {
		raw = cvp.Value
	}

	// compare raw (pre-conversion) values, if unchanged, skip this update. note that
	// `usingDefaultValue` is equal to itself but nothing else.
	if reflect.DeepEqual(sub.raw, raw) {
		// make raw field point to new one, not old one, so that old loaded files can get
		// garbage collected.
		sub.raw = raw
		return
	}

	// raw value changed, need to dispatch default or converted value
	var newVal T
	if cvp == nil {
		newVal = sub.def
	} else {
		newVal, err = convertWithCache(c, key, convert, cvp)
		if err != nil {
			// We failed to convert the value to the desired type. Use the default.
			if c.throttleLog() {
				c.logger.Warn("Failed to convert value, using default", tag.Key(key.String()), tag.IgnoredValue(cvp), tag.Error(err))
			}
			newVal, raw = sub.def, usingDefaultValue
		}
	}

	sub.raw = raw
	c.callbackPool.Do(func() { sub.f(newVal) })
}

// called with subscriptionLock
func dispatchUpdateWithConstrainedDefault[T any](
	c *Collection,
	key Key,
	convert func(value any) (T, error),
	sub *subscription[T],
	cvs []ConstrainedValue,
) {
	// Note: This performs the conversion even if the raw value is unchanged. This isn't ideal,
	// but so far constrained default settings are only used for primitive values so it's okay.
	// If we have a constrained default value with a complex conversion function, this could be
	// optimized to delay conversion until after we check DeepEqual.
	newVal, raw := findAndResolveWithConstrainedDefaults(c, key, convert, cvs, sub.cdef, sub.prec)

	// compare raw (pre-conversion) values, if unchanged, skip this update. note that
	// `usingDefaultValue` is equal to itself but nothing else.
	if reflect.DeepEqual(sub.raw, raw) {
		// make raw field point to new one, not old one, so that old loaded files can get
		// garbage collected.
		sub.raw = raw
		return
	}

	sub.raw = raw
	c.callbackPool.Do(func() { sub.f(newVal) })
}

func convertWithCache[T any](c *Collection, key Key, convert func(any) (T, error), cvp *ConstrainedValue) (T, error) {
	weakcvp := weak.Make(cvp)

	if converted, ok := c.convertCache.Load(weakcvp); ok {
		if t, ok := converted.(T); ok {
			return t, nil
		}
		// Each key can only be used with a single type, so this shouldn't happen
		c.logger.Warn("Cached converted value has wrong type", tag.Key(key.String()))
		// Fall through to regular conversion
	}

	t, err := convert(cvp.Value)
	if err != nil {
		var zero T
		return zero, err
	}

	c.convertCache.Store(weakcvp, t)

	runtime.AddCleanup(cvp, func(w weak.Pointer[ConstrainedValue]) {
		c.convertCache.Delete(w)
	}, weakcvp)

	return t, nil
}

func convertInt(val any) (int, error) {
	switch val := val.(type) {
	case int:
		return int(val), nil
	case int8:
		return int(val), nil
	case int16:
		return int(val), nil
	case int32:
		return int(val), nil
	case int64:
		return int(val), nil
	case uint:
		return int(val), nil
	case uint8:
		return int(val), nil
	case uint16:
		return int(val), nil
	case uint32:
		return int(val), nil
	case uint64:
		return int(val), nil
	case uintptr:
		return int(val), nil
	default:
		return 0, errors.New("value type is not int")
	}
}

func convertFloat(val any) (float64, error) {
	switch val := val.(type) {
	case float32:
		return float64(val), nil
	case float64:
		return float64(val), nil
	}
	if ival, err := convertInt(val); err == nil {
		return float64(ival), nil
	}
	return 0, errors.New("value type is not float64")
}

func convertDuration(val any) (time.Duration, error) {
	switch v := val.(type) {
	case time.Duration:
		return v, nil
	case string:
		d, err := timestamp.ParseDurationDefaultSeconds(v)
		if err != nil {
			return 0, fmt.Errorf("failed to parse duration: %v", err)
		}
		return d, nil
	}
	// treat numeric values as seconds
	if ival, err := convertInt(val); err == nil {
		return time.Duration(ival) * time.Second, nil
	} else if fval, err := convertFloat(val); err == nil {
		return time.Duration(fval * float64(time.Second)), nil
	}
	return 0, errors.New("value not convertible to Duration")
}

func convertString(val any) (string, error) {
	if stringVal, ok := val.(string); ok {
		return stringVal, nil
	}
	return "", errors.New("value type is not string")
}

func convertBool(val any) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return false, errors.New("value type is not bool")
	}
}

func convertMap(val any) (map[string]any, error) {
	if mapVal, ok := val.(map[string]any); ok {
		return mapVal, nil
	}
	return nil, errors.New("value type is not map")
}

// ConvertStructure can be used as a conversion function for New*TypedSettingWithConverter.
// The value from dynamic config will be converted to T, on top of the given default.
//
// Note that any failure in conversion of _any_ field will result in the overall default being used,
// ignoring the fields that successfully converted.
//
// Note that the default value will be deep-copied and then passed to mapstructure with the
// ZeroFields setting false, so the config value will be _merged_ on top of it. Be very careful
// when using non-empty maps or slices, the result may not be what you want.
//
// To avoid confusion, the default passed to ConvertStructure should be either the same as the
// overall default for the setting (if you want any value set to be merged over the default, i.e.
// treat the fields independently), or the zero value of its type (if you want to treat the fields
// as a group and default unset fields to zero).
func ConvertStructure[T any](def T) func(v any) (T, error) {
	return func(v any) (T, error) {
		// if we already have the right type, no conversion is necessary
		if typedV, ok := v.(T); ok {
			return typedV, nil
		}

		// Deep-copy the default and decode over it. This allows using e.g. a struct with some
		// default fields filled in and a config that only set some fields.
		out := deepCopyForMapstructure(def)

		dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
			Result: &out,
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				mapstructureHookDuration,
				mapstructureHookProtoEnum,
				mapstructureHookGeneric,
			),
		})
		if err != nil {
			return out, err
		}
		err = dec.Decode(v)
		return out, err
	}
}

// Parses string into time.Duration. mapstructure has an implementation of this already but it
// calls time.ParseDuration and we want to use our own method.
func mapstructureHookDuration(f, t reflect.Type, data any) (any, error) {
	if t != durationType {
		return data, nil
	}
	return convertDuration(data)
}

// Parses proto enum values from strings.
func mapstructureHookProtoEnum(f, t reflect.Type, data any) (any, error) {
	if f != stringType || !t.Implements(protoEnumType) {
		return data, nil
	}
	vals := reflect.New(t).Interface().(protoreflect.Enum).Descriptor().Values()
	str := strings.ToLower(data.(string)) // we checked f above so this can't fail
	for i := 0; i < vals.Len(); i++ {
		val := vals.Get(i)
		if str == strings.ToLower(string(val.Name())) {
			return val.Number(), nil
		}
	}
	return nil, fmt.Errorf("name %q not found in enum %s", data, t.Name())
}

// Parses generic values. See GenericParseHook.
func mapstructureHookGeneric(f, t reflect.Type, data any) (any, error) {
	if mth, ok := t.MethodByName("DynamicConfigParseHook"); ok &&
		mth.Func.IsValid() &&
		mth.Type != nil &&
		mth.Type.NumIn() == 2 &&
		mth.Type.In(1) == f &&
		mth.Type.NumOut() == 2 &&
		mth.Type.Out(0) == t &&
		mth.Type.Out(1) == errorType {

		out := mth.Func.Call([]reflect.Value{reflect.Zero(t), reflect.ValueOf(data)})
		if !out[1].IsNil() {
			if err, ok := out[1].Interface().(error); ok {
				return nil, err
			}
			return nil, errors.New("failed to convert DynamicConfigParseHook error")
		}
		return out[0].Interface(), nil
	}

	// pass through
	return data, nil
}
