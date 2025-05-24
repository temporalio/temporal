package stamp

import (
	"context"
	"fmt"
	reflect "reflect"
	"strings"
	"sync"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
)

type (
	ModelEnv struct {
		log.Logger
		root Root
		testEnv
		router   routerWrapper
		modelSet ModelSet

		routeLock   sync.Mutex
		modelIdx    map[modelKey]modelWrapper
		childrenIdx map[modelKey][]modelKey

		callbackLock        sync.RWMutex
		triggerCallbacksIdx map[ActID]func(routableAction, modelWrapper)
	}
	modelEnv interface {
		log.Logger
		onMatched(ActID, func(act routableAction, mdl modelWrapper)) func()
		getPathTo(reflect.Type) []modelType
		consume(reflect.Type, *internalModel, ID, routableAction) (modelWrapper, func(reflect.Value))
		updateIdentity(modelKey, modelKey)
		getTestEnv() testEnv
		getRequire() *require.Assertions
	}
	testEnv interface {
		Context() context.Context
		Logger() log.Logger
		Assertions() *require.Assertions
		genContext() *genContext
	}
	modelKey string
)

func NewModelEnv(
	testEnv testEnv,
	modelSet ModelSet,
	router routerWrapper,
) *ModelEnv {
	res := &ModelEnv{
		router:              router,
		Logger:              newLogger(testEnv.Logger()),
		testEnv:             testEnv,
		modelSet:            modelSet,
		modelIdx:            make(map[modelKey]modelWrapper),
		childrenIdx:         make(map[modelKey][]modelKey),
		triggerCallbacksIdx: make(map[ActID]func(routableAction, modelWrapper)),
	}
	initRouter(router, res)
	res.root = Root{env: res}
	res.modelIdx[res.root.getKey()] = &res.root
	return res
}

func (e *ModelEnv) Route(act routableAction) OnComplete {
	defer func() {
		if r := recover(); r != nil {
			softassert.Fail(e, fmt.Sprintf("%v", r))
		}
	}()

	e.routeLock.Lock()
	defer e.routeLock.Unlock()

	err := validator.Struct(act)
	if err != nil {
		panic(fmt.Sprintf("action failed validation: %v", err))
	}

	var onMatchedFunc func(routableAction, modelWrapper)
	e.callbackLock.RLock()
	if cb, ok := e.triggerCallbacksIdx[act.ID()]; ok {
		onMatchedFunc = cb
	}
	e.callbackLock.RUnlock()

	onComplete := e.router.getRouter().route(act, onMatchedFunc)
	if onComplete == nil {
		return nil
	}

	// verify all models
	e.verify()

	return func(act action) {
		defer func() {
			if r := recover(); r != nil {
				softassert.Fail(e, fmt.Sprintf("%v", r))
			}
		}()

		e.routeLock.Lock()
		defer e.routeLock.Unlock()

		// consume the response action
		onComplete(act)

		// verify all models again after completion
		e.verify()
	}
}

func (e *ModelEnv) consume(
	ty reflect.Type,
	parent *internalModel,
	id ID,
	act routableAction,
) (modelWrapper, func(reflect.Value)) {
	// resolve the model type
	qualTypeName := qualifiedTypeName(ty.Elem())
	mdlType, ok := e.modelSet.typeByQualifiedName[qualTypeName]
	if !ok {
		panic(fmt.Sprintf("model type %q not found", qualTypeName))
	}

	// identify the parent
	parentKey := parent.getKey()
	parentWrapper := e.modelIdx[parentKey]
	if parentWrapper == nil {
		panic(fmt.Sprintf("parent model %q not found", parentKey))
	}

	// get or create the model
	var child modelWrapper
	childKey := newModelKey(parentKey, mdlType, id)
	if child, ok = e.modelIdx[childKey]; !ok {
		child = newModel(e, id, mdlType, parentWrapper)
		e.Info(fmt.Sprintf("%s created by %s", boldStr(childKey), act),
			tag.NewStringTag("parent", parent.str()),
			triggerTag(act.ID()))
		e.modelIdx[child.getKey()] = child
		e.childrenIdx[parent.getKey()] = append(e.childrenIdx[parent.getKey()], child.getKey())
	}

	// consume action by the model
	// TODO: index this at init instead
	var callback func(reflect.Value)
	childValue := reflect.ValueOf(child)
	childType := childValue.Type()
	actType := reflect.TypeOf(act)
	for i := 0; i < childType.NumMethod(); i++ {
		method := childType.Method(i)
		if !strings.HasPrefix(method.Name, "On") {
			continue
		}
		methodType := method.Type
		if methodType.NumIn() != 2 {
			continue
		}
		firstArgType := methodType.In(1)
		if firstArgType == actType {
			ret := method.Func.Call([]reflect.Value{childValue, reflect.ValueOf(act)})
			if len(ret) > 0 {
				callback = func(act reflect.Value) {
					ret[0].Call([]reflect.Value{act})
				}
			}
		}
	}

	return child, callback
}

func (e *ModelEnv) updateIdentity(oldKey modelKey, newKey modelKey) {
	mw := e.modelIdx[oldKey]
	e.modelIdx[newKey] = mw
	// no need to update childrenIdx as the old key will still point to the model
}

func (e *ModelEnv) getRequire() *require.Assertions {
	return e.testEnv.Assertions()
}

//func (e *ModelEnv) HandleAction(triggerID ActID, action A) {
//	defer func() {
//		// recover from panics to not disturb the test
//		if r := recover(); r != nil {
//			softassert.Fail(e, fmt.Sprintf("%v", r))
//		}
//	}()
//
//	e.routeContextLock.Lock()
//	defer func() {
//		e.routeContextLock.Unlock()
//	}()
//
//	aw := newActionWrapper(triggerID, action)
//
//	// update models
//	var matchLog []string
//	e.walk(&e.root, func(parent, child modelWrapper) bool {
//		if child == nil {
//			e.createNewChildren(parent, aw)
//			return true
//		}
//
//		if matched := e.updateExistingChild(parent, child, aw); matched {
//			matchLog = append(matchLog, fmt.Sprintf("\t✓ %s", boldStr(child.str())))
//			return true
//		} else {
//			matchLog = append(matchLog, fmt.Sprintf("\tx %s", redStr(child.str())))
//		}
//		return false
//	})
//
//	e.Info(fmt.Sprintf("%s matches: \n%s",
//		action.String(),
//		strings.Join(matchLog, "\n"),
//	), triggerTag(triggerID))
//}

// TODO: infinite loop?
func (e *ModelEnv) verify() {
	//e.walk(&e.root, func(_, child modelWrapper) bool {
	//	if child == nil {
	//		return false
	//	}
	//	for _, verifyFn := range e.modelSet.propertiesOf(child) {
	//		p, res, err := verifyFn(child)
	//		if err != nil {
	//			e.Error(err.Error())
	//		}
	//		if res != true {
	//			e.Error(propErr{prop: p.String(), err: PropViolated}.Error())
	//		}
	//	}
	//	return true
	//})
}

//func (e *ModelEnv) handleAction(id ActID, action any) {
//	e.HandleAction(id, action.(A))
//}

//func (e *ModelEnv) createNewChildren(
//	parent modelWrapper,
//	action *actionWrapper,
//) {
//	for _, mdlType := range e.modelSet.childTypesOf(parent) {
//		// check identity (using an example model to not create a new one)
//		exampleMdl := e.modelSet.exampleOf(mdlType)
//		exampleMdl.setScope(parent)
//		id := e.modelSet.identify(exampleMdl, action)
//		if id == "" {
//			// unable to identify
//			continue
//		}
//		newChildKey := newModelKey(parent.getKey(), mdlType, id)
//		if _, ok := e.modelIdx[newChildKey]; ok {
//			// already exists
//			continue
//		}
//
//		// create new model
//		newChild := newModel(e, mdlType, parent)
//		newChild.updateIdentity(parent.getKey(), id)
//		if newChild.getKey() != newChildKey { // have to run it again for side effects
//			e.Fatal(fmt.Sprintf("identity mismatch: expected %q, but got %q", newChildKey, newChild.getKey()))
//		}
//		e.Info(fmt.Sprintf("%s created by %s", boldStr(newChildKey), action),
//			tag.NewStringTag("parent", parent.str()),
//			triggerTag(action.triggerID))
//
//		// index new model
//		e.modelIdx[newChild.getKey()] = newChild
//		e.childrenIdx[parent.getKey()] = append(e.childrenIdx[parent.getKey()], newChild.getKey())
//
//		// no need to do anything else here!
//		// child will be visited next automatically
//	}
//}

//func (e *ModelEnv) updateExistingChild(
//	parent, child modelWrapper,
//	action *actionWrapper,
//) bool {
//	// check identity
//	id := e.modelSet.identify(child, action)
//	if id == "" {
//		// unable to identify
//		return false
//	}
//	if id != child.GetID() {
//		// not the same model instance
//		return false
//	}
//
//	// invoke action handler
//	e.modelSet.consume(child, action)
//
//	// invoke trigger callback
//	if action.triggerID != "" {
//		e.callbackLock.RLock()
//		if cb, ok := e.triggerCallbacksIdx[action.triggerID]; ok {
//			cb(action, child)
//		}
//		e.callbackLock.RUnlock()
//	}
//
//	return true
//}

func (e *ModelEnv) walk(start modelWrapper, fn func(modelWrapper, modelWrapper) bool) {
	var walkModelsFn func(modelWrapper)
	walkModelsFn = func(parent modelWrapper) {
		fn(parent, nil) // might create new children

		for _, childKey := range e.childrenIdx[parent.getKey()] {
			child := e.modelIdx[childKey]
			if matched := fn(parent, child); matched {
				walkModelsFn(child)
			}
		}
	}
	walkModelsFn(start)
}

func (e *ModelEnv) getPathTo(ty reflect.Type) []modelType {
	return e.modelSet.pathTo(ty)
}

func (e *ModelEnv) Root() *Root {
	return &e.root
}

func (e *ModelEnv) onMatched(triggerID ActID, fn func(routableAction, modelWrapper)) func() {
	e.callbackLock.Lock()
	defer e.callbackLock.Unlock()
	e.triggerCallbacksIdx[triggerID] = fn

	return func() {
		e.callbackLock.Lock()
		defer e.callbackLock.Unlock()
		delete(e.triggerCallbacksIdx, triggerID)
	}
}

func (e *ModelEnv) getTestEnv() testEnv {
	return e.testEnv
}
