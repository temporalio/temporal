package stamp

import (
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
)

var (
	scopeTypePattern   = regexp.MustCompile(`Scope\[(.*)]`)
	embeddableMdlTypes = []reflect.Type{
		reflect.TypeOf(&Model[Root]{}).Elem(),
		reflect.TypeOf(&Scope[Root]{}).Elem(),
	}
)

type (
	ModelSet struct {
		typeIdx             map[modelType]struct{}
		typeByQualifiedName map[string]modelType
		childTypes          map[modelType][]modelType
		parentType          map[modelType]modelType
		ruleIdx             map[modelType][]func(modelWrapper) (prop, any, error)
		identHandlers       map[modelType]func(modelWrapper, *actionWrapper) ID
		actionHandlers      map[modelType]func(modelWrapper, *actionWrapper)
	}
	registerModel interface {
		modelWrapper
	}
)

func NewModelSet() *ModelSet {
	return &ModelSet{
		typeIdx:             make(map[modelType]struct{}),
		typeByQualifiedName: map[string]modelType{rootTypeName: rootType},
		childTypes:          make(map[modelType][]modelType),
		parentType:          make(map[modelType]modelType),
		ruleIdx:             make(map[modelType][]func(modelWrapper) (prop, any, error)),
		identHandlers:       make(map[modelType]func(modelWrapper, *actionWrapper) ID),
		actionHandlers:      make(map[modelType]func(modelWrapper, *actionWrapper)),
	}
}

// TODO: check that struct has no fields at all (we don't want any state there)
// TODO: type check parent matches model somehow?
// TODO: fail when 2 params of a handler have the same type
// TODO: check if handler contains unexpected parameter types (ie from outside of this context)
func RegisterModel[M registerModel](set *ModelSet) {
	ptrType := reflect.TypeFor[M]()
	mdlType := modelType{ptrType: ptrType, structType: ptrType.Elem(), name: ptrType.Elem().Name()}

	if _, ok := set.typeIdx[mdlType]; ok {
		panic(fmt.Sprintf("%q already registered", mdlType.name))
	}
	set.typeIdx[mdlType] = struct{}{}
	set.typeByQualifiedName[qualifiedTypeName(mdlType.structType)] = mdlType

	for i := 0; i < mdlType.structType.NumField(); i++ {
		field := mdlType.structType.Field(i)
		fieldTypeName := field.Type.String()
		parentMatch := scopeTypePattern.FindStringSubmatch(fieldTypeName)
		if len(parentMatch) == 2 {
			scopeFieldMdlTypeName := strings.TrimPrefix(parentMatch[1], "*")
			scopeFieldMdlType, ok := set.typeByQualifiedName[scopeFieldMdlTypeName]
			if !ok {
				panic(fmt.Sprintf("scope %q from model %q must be a registered model", scopeFieldMdlTypeName, mdlType.name))
			}
			set.childTypes[scopeFieldMdlType] = append(set.childTypes[scopeFieldMdlType], mdlType)
			set.parentType[mdlType] = scopeFieldMdlType
		}
	}

	// register model handlers
	// TODO: scan unexported methods, too, in case user accidentally made one private
	mdlInst := reflect.New(mdlType.structType).Interface().(modelWrapper)
	mdlInstVal := reflect.ValueOf(mdlInst)
	for i := 0; i < ptrType.NumMethod(); i++ {
		method := ptrType.Method(i)
		if isFromEmbedded(method) {
			continue
		}

		switch {
		case method.Name == "Identify":
			set.identHandlers[mdlType] = func(mw modelWrapper, aw *actionWrapper) ID {
				return method.Func.Call([]reflect.Value{
					reflect.ValueOf(mw),
					reflect.ValueOf(aw.action),
				})[0].Interface().(ID)
			}

		case method.Name == "Consume":
			set.actionHandlers[mdlType] = func(mw modelWrapper, aw *actionWrapper) {
				method.Func.Call([]reflect.Value{
					reflect.ValueOf(mw),
					reflect.ValueOf(aw.action),
				})
			}

		case method.Type.NumOut() == 1 && method.Type.Out(0).Implements(propType):
			if method.Type.NumIn() != 1 {
				panic(fmt.Sprintf("property %q on %q must not have any parameters", method.Name, mdlType.name))
			}
			if method.Type.NumOut() != 1 {
				panic(fmt.Sprintf("property %q on %q must return `Prop` or `Rule`", method.Name, mdlType.name))
			}
			newProp := method.Func.Call([]reflect.Value{mdlInstVal})[0].Interface().(prop)
			newProp.setName(fmt.Sprintf("%s.%s", mdlType.name, method.Name))
			if err := newProp.Validate(); err != nil {
				panic(fmt.Sprintf("property %q on %q failed validation: %v", method.Name, mdlType.name, err))
			}

			// index properties that are rules
			if method.Type.Out(0) == ruleType {
				set.ruleIdx[mdlType] = append(set.ruleIdx[mdlType], func(mw modelWrapper) (prop, any, error) {
					res, err := newProp.eval(mw.getPropCtx())
					return newProp, res, err
				})
			}
		}
	}
}

func (s ModelSet) identify(mw modelWrapper, action *actionWrapper) ID {
	return s.identHandlers[mw.getType()](mw, action)
}

func (s ModelSet) consume(mw modelWrapper, action *actionWrapper) {
	s.actionHandlers[mw.getType()](mw, action)
}

func (s ModelSet) validate(a any) {
	if reflect.TypeOf(a).Kind() != reflect.Struct {
		panic(fmt.Sprintf("expected struct, got %T", a))
	}
	err := validator.Struct(a)
	if err != nil {
		// TODO: use logger
		panic(fmt.Sprintf("%T failed validation: %v", a, err))
	}
}

func (s ModelSet) childTypesOf(m modelWrapper) []modelType {
	return s.childTypes[m.getType()]
}

func (s ModelSet) propertiesOf(m modelWrapper) []func(m modelWrapper) (prop, any, error) {
	return s.ruleIdx[m.getType()]
}

func (s ModelSet) pathTo(ty reflect.Type) []modelType {
	typeName := qualifiedTypeName(ty)
	curType, ok := s.typeByQualifiedName[typeName]
	if !ok {
		panic(fmt.Sprintf("type %q is not a registered model", ty))
	}

	var res []modelType
	for curType != rootType {
		res = append(res, curType)
		curType = s.parentType[curType]
	}
	slices.Reverse(res)
	return res
}

func isFromEmbedded(method reflect.Method) bool {
	for _, typ := range embeddableMdlTypes {
		if _, ok := typ.MethodByName(method.Name); ok {
			return true
		}
	}
	return false
}
