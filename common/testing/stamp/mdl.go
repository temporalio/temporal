package stamp

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/stretchr/testify/require"
)

const aliasIDPrefix = "alias:"

type (
	Model[T modelWrapper] struct {
		*internalModel
		Require *require.Assertions
	}
	internalModel struct { // model representation without type annotations
		key      modelKey
		domainID ID
		mdlEnv   modelEnv
		typeOf   modelType
		scenario *Scenario
		genCache map[any]any
	}
	modelType struct {
		ptrType    reflect.Type
		structType reflect.Type
		name       string
	}
	modelAccessor interface {
		getModel() *internalModel // read-only!
		GetID() ID
	}
	modelWrapper interface { // API for the user-defined struct
		modelAccessor
		str() string
		setScope(modelWrapper)
		getScope() modelWrapper
		getType() modelType
		setModel(*internalModel)
		getKey() modelKey
		Verify() // implemented by the user
	}
	Scope[T modelWrapper] struct {
		mw T
	}
	ID string
)

func NewAliasID(id string) ID {
	return ID(fmt.Sprintf("%s%s", aliasIDPrefix, id))
}

// Note: not `String` to avoid concurrency issues when being printed by testing goroutine
func (m *internalModel) str() string {
	return fmt.Sprintf("%s[%s]", m.typeOf.name, m.getID())
}

func (m *Model[T]) setModel(mdl *internalModel) {
	m.internalModel = mdl
	m.Require = mdl.mdlEnv.getRequire()
}

func (m *Model[T]) getModelAccessor() modelAccessor {
	return m.getModel()
}

func (m *Model[T]) SetID(id string) {
	if id == "" {
		panic("cannot set empty ID")
	}
	if !strings.HasPrefix(string(m.domainID), aliasIDPrefix) {
		panic(fmt.Sprintf("cannot set ID %q, non-alias ID is already set", id))
	}
	prevKey := m.key
	m.updateIdentity(m.key, ID(id))
	m.mdlEnv.updateIdentity(prevKey, m.key)
}

func (m *internalModel) getEnv() modelEnv {
	return m.mdlEnv
}

func (m *internalModel) getType() modelType {
	return m.typeOf
}

func (m *internalModel) getID() ID {
	return m.domainID
}

func (m *internalModel) updateIdentity(scopeKey modelKey, id ID) {
	m.domainID = id
	m.key = newModelKey(scopeKey, m.typeOf, id)
}

func newModelKey(scopeKey modelKey, mt modelType, id ID) modelKey {
	return modelKey(fmt.Sprintf("%s/%s[%s]", scopeKey, mt.name, id))
}

// GetID returns the domain ID of the model.
func (m *internalModel) GetID() ID {
	return m.getID()
}

func (m *internalModel) getModel() *internalModel {
	return m
}

func (m *internalModel) setModel(_ *internalModel) {
	panic("not implemented")
}

func (m *internalModel) is_model() {
	panic("not implemented")
}

func (m *internalModel) getKey() modelKey {
	return m.key
}

func (id ID) DefaultGen() Gen[ID] {
	return GenName[ID]()
}

func (s Scope[T]) getScope() modelWrapper {
	return s.mw
}

func (s Scope[T]) GetScope() T {
	return s.mw
}

func (s *Scope[T]) setScope(mw modelWrapper) {
	s.mw = mw.(T)
}

func (t modelType) String() string {
	return t.name
}
