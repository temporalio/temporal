package stamp

import (
	"context"
	"reflect"

	"go.temporal.io/server/common/log"
)

var (
	RootMdl      = Model[Root]{internalModel: Root{}.getModel()}
	rootPtrType  = reflect.TypeFor[*Root]()
	rootTypeName = qualifiedTypeName(rootPtrType.Elem())
	rootType     = modelType{
		ptrType:    rootPtrType,
		structType: rootPtrType.Elem(),
		name:       rootTypeName,
	}
)

type Root struct {
	env        modelEnv
	onActionFn func(ActionParams) error
}

func (r Root) GetLogger() log.Logger {
	panic("not supported")
}

func (r Root) getDomainKey() ID {
	return "<root>"
}

func (r Root) updateIdentity(scopeKey modelKey, id ID) {
	panic("not supported")
}

func (r Root) getEnv() modelEnv {
	return r.env
}

func (r Root) setModel(_ *internalModel) {
	panic("not supported")
}

func (r Root) str() string {
	return "Root[]"
}

func (r Root) getModel() *internalModel {
	return &internalModel{
		mdlEnv: r.env,
		key:    r.getKey(),
		// anything else is unavailable
	}
}

func (r Root) getScope() modelWrapper {
	panic("not supported")
}

func (r Root) getType() modelType {
	return rootType
}

func (r Root) Verify() {}

func (r Root) getKey() modelKey {
	return "/"
}

func (r Root) GetID() ID {
	panic("not supported")
}

func (r Root) getModelAccessor() *Root {
	return &r
}

func (r Root) setScope(modelWrapper) {
	panic("not supported")
}

func (r Root) OnAction(
	ctx context.Context,
	actionParams ActionParams,
) error {
	return r.onActionFn(actionParams)
}

func (r *Root) SetActionHandler(fn func(ActionParams) error) {
	r.onActionFn = fn
}
