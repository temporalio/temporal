package chasm

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

var (
	// This is golang type identifier regex.
	nameValidator = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type (
	Registry struct {
		libraries         map[string]Library                     // library name -> library
		componentByType   map[string]*RegistrableComponent       // fully qualified type name -> component
		componentByGoType map[reflect.Type]*RegistrableComponent // component go type -> component

		taskByType   map[string]*RegistrableTask       // fully qualified type name -> task
		taskByGoType map[reflect.Type]*RegistrableTask // task go type -> task

		logger log.Logger
	}
)

func NewRegistry(logger log.Logger) *Registry {
	return &Registry{
		libraries:         make(map[string]Library),
		componentByType:   make(map[string]*RegistrableComponent),
		componentByGoType: make(map[reflect.Type]*RegistrableComponent),
		taskByType:        make(map[string]*RegistrableTask),
		taskByGoType:      make(map[reflect.Type]*RegistrableTask),
		logger:            logger,
	}
}

func (r *Registry) Register(lib Library) error {
	if err := r.validateName(lib.Name()); err != nil {
		return err
	}
	if _, ok := r.libraries[lib.Name()]; ok {
		return fmt.Errorf("library %s is already registered", lib.Name())
	}
	r.libraries[lib.Name()] = lib

	for _, c := range lib.Components() {
		if err := r.registerComponent(lib, c); err != nil {
			return err
		}
	}
	for _, t := range lib.Tasks() {
		if err := r.registerTask(lib, t); err != nil {
			return err
		}
	}
	return nil
}

// RegisterServices registers all gRPC services from all registered libraries.
func (r *Registry) RegisterServices(server *grpc.Server) {
	for _, lib := range r.libraries {
		lib.RegisterServices(server)
	}
}

func (r *Registry) component(fqn string) (*RegistrableComponent, bool) {
	rc, ok := r.componentByType[fqn]
	return rc, ok
}

func (r *Registry) task(fqn string) (*RegistrableTask, bool) {
	rt, ok := r.taskByType[fqn]
	return rt, ok
}

func (r *Registry) componentFor(componentInstance any) (*RegistrableComponent, bool) {
	rt, ok := r.componentByGoType[reflect.TypeOf(componentInstance)]
	return rt, ok
}

func (r *Registry) taskFor(taskInstance any) (*RegistrableTask, bool) {
	rt, ok := r.taskByGoType[reflect.TypeOf(taskInstance)]
	return rt, ok
}

func (r *Registry) componentOf(componentGoType reflect.Type) (*RegistrableComponent, bool) {
	rc, ok := r.componentByGoType[componentGoType]
	return rc, ok
}

func (r *Registry) taskOf(taskGoType reflect.Type) (*RegistrableTask, bool) {
	rt, ok := r.taskByGoType[taskGoType]
	return rt, ok
}

func (r *Registry) registerComponent(
	lib namer,
	rc *RegistrableComponent,
) error {
	if err := r.validateName(rc.componentType); err != nil {
		return err
	}
	fqn := fullyQualifiedName(lib.Name(), rc.componentType)
	if _, ok := r.componentByType[fqn]; ok {
		return fmt.Errorf("component %s is already registered", fqn)
	}
	if rc.library != nil {
		return fmt.Errorf("component %s is already registered in library %s", fqn, rc.library.Name())
	}
	// rc.goType implements Component interface; therefore, it must be a struct.
	// This check to protect against the interface itself being registered.
	if !(rc.goType.Kind() == reflect.Struct ||
		(rc.goType.Kind() == reflect.Ptr && rc.goType.Elem().Kind() == reflect.Struct)) {
		return fmt.Errorf("component type %s must be struct or pointer to struct", rc.goType.String())
	}
	if _, ok := r.componentByGoType[rc.goType]; ok {
		return fmt.Errorf("component type %s is already registered", rc.goType.String())
	}
	r.warnUnmanagedFields(fqn, rc)

	rc.library = lib
	r.componentByType[fqn] = rc
	r.componentByGoType[rc.goType] = rc
	return nil
}
func (r *Registry) registerTask(
	lib namer,
	rt *RegistrableTask,
) error {
	if err := r.validateName(rt.taskType); err != nil {
		return err
	}
	fqn := fullyQualifiedName(lib.Name(), rt.taskType)
	if _, ok := r.taskByType[fqn]; ok {
		return fmt.Errorf("task %s is already registered", fqn)
	}
	if rt.library != nil {
		return fmt.Errorf("task %s is already registered in library %s", fqn, rt.library.Name())
	}
	if !(rt.goType.Kind() == reflect.Struct ||
		(rt.goType.Kind() == reflect.Ptr && rt.goType.Elem().Kind() == reflect.Struct)) {
		return fmt.Errorf("task type %s must be struct or pointer to struct", rt.goType.String())
	}
	if _, ok := r.taskByGoType[rt.goType]; ok {
		return fmt.Errorf("task type %s is already registered", rt.goType.String())
	}
	if !(rt.componentGoType.Kind() == reflect.Interface ||
		(rt.componentGoType.Kind() == reflect.Struct ||
			(rt.componentGoType.Kind() == reflect.Ptr && rt.componentGoType.Elem().Kind() == reflect.Struct)) &&
			rt.componentGoType.AssignableTo(reflect.TypeOf((*Component)(nil)).Elem())) {
		return fmt.Errorf("component type %s must be and interface or struct that implements Component interface", rt.componentGoType.String())
	}

	rt.library = lib
	r.taskByType[fqn] = rt
	r.taskByGoType[rt.goType] = rt
	return nil
}

func (r *Registry) validateName(n string) error {
	if n == "" {
		return errors.New("name must not be empty")
	}
	if !nameValidator.MatchString(n) {
		return fmt.Errorf("name %s is invalid. name must follow golang identifier rules: %s", n, nameValidator.String())
	}
	return nil
}

func (r *Registry) warnUnmanagedFields(fqn string, rc *RegistrableComponent) {
	var unmanagedFields []string
	for f := range unmanagedFieldsOf(rc.goType) {
		unmanagedFields = append(unmanagedFields, fmt.Sprintf("%s %s", f.name, f.typ))
	}
	if len(unmanagedFields) > 0 {
		r.logger.Info(fmt.Sprintf(
			"Warning: CHASM component %s declares state fields that won't be managed by CHASM:\n\t%s",
			fqn,
			strings.Join(unmanagedFields, "\n\t")))
	}
}
