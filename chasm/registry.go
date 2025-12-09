package chasm

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"strings"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

var (
	// This is golang type identifier regex.
	nameValidator = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type (
	Registry struct {
		libraries map[string]Library // library name -> library

		// rc stands for RegistrableComponent.
		rcByFqn    map[string]*RegistrableComponent       // fully qualified type name -> component
		rcByID     map[uint32]*RegistrableComponent       // component type ID -> component
		rcByGoType map[reflect.Type]*RegistrableComponent // component go type -> component

		// rt stands for RegistrableTask.
		rtByFqn    map[string]*RegistrableTask       // fully qualified type name -> task
		rtByID     map[uint32]*RegistrableTask       // task type ID -> task
		rtByGoType map[reflect.Type]*RegistrableTask // task go type -> task

		nexusServices          map[string]*nexus.Service // service name -> nexus service
		NexusEndpointProcessor *NexusEndpointProcessor

		logger log.Logger
	}
)

func NewRegistry(logger log.Logger) *Registry {
	return &Registry{
		libraries:              make(map[string]Library),
		rcByFqn:                make(map[string]*RegistrableComponent),
		rcByID:                 make(map[uint32]*RegistrableComponent),
		rcByGoType:             make(map[reflect.Type]*RegistrableComponent),
		rtByFqn:                make(map[string]*RegistrableTask),
		rtByID:                 make(map[uint32]*RegistrableTask),
		rtByGoType:             make(map[reflect.Type]*RegistrableTask),
		nexusServices:          make(map[string]*nexus.Service),
		NexusEndpointProcessor: NewNexusEndpointProcessor(),
		logger:                 logger,
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

	for _, svc := range lib.NexusServices() {
		if err := r.registerNexusService(svc); err != nil {
			return err
		}
	}

	for _, svc := range lib.NexusServiceProcessors() {
		if err := r.NexusEndpointProcessor.RegisterServiceProcessor(svc); err != nil {
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

// ComponentFqnByID converts component type ID to fully qualified component type name.
// This method should only be used by CHASM framework internal code,
// NOT CHASM library developers.
func (r *Registry) ComponentFqnByID(id uint32) (string, bool) {
	rc, ok := r.rcByID[id]
	if !ok {
		return "", false
	}
	return rc.fqType(), true
}

// ComponentIDByFqn converts fully qualified component type name to component type ID.
// This method should only be used by CHASM framework internal code,
// NOT CHASM library developers.
func (r *Registry) ComponentIDByFqn(fqn string) (uint32, bool) {
	rc, ok := r.rcByFqn[fqn]
	if !ok {
		return 0, false
	}
	return rc.componentID, true
}

// ComponentByID returns the registrable component for a given archetype ID.
// This method should only be used by CHASM framework internal code,
// NOT CHASM library developers.
func (r *Registry) ComponentByID(id uint32) (*RegistrableComponent, bool) {
	rc, ok := r.rcByID[id]
	return rc, ok
}

// ComponentIDFor converts registered component instance to component type ID.
// This method should only be used by CHASM framework internal code,
// NOT CHASM library developers.
func (r *Registry) ComponentIDFor(componentInstance any) (uint32, bool) {
	rc, ok := r.componentFor(componentInstance)
	if !ok {
		return 0, false
	}
	return rc.componentID, true
}

// TaskByID returns the registrable task for a given task type ID.
// This method should only be used by CHASM framework internal code,
// NOT CHASM library developers.
func (r *Registry) TaskByID(id uint32) (*RegistrableTask, bool) {
	rt, ok := r.rtByID[id]
	return rt, ok
}

// TaskFqnByID converts task type ID to fully qualified task type name.
// This method should only be used by CHASM framework internal code,
// NOT CHASM library developers.
func (r *Registry) TaskFqnByID(id uint32) (string, bool) {
	rt, ok := r.rtByID[id]
	if !ok {
		return "", false
	}
	return rt.fqType(), true
}

// TaskIDFor converts registered task instance to task type ID.
// This method should only be used by CHASM framework internal code,
// NOT CHASM library developers.
func (r *Registry) TaskIDFor(taskInstance any) (uint32, bool) {
	rt, ok := r.taskFor(taskInstance)
	if !ok {
		return 0, false
	}
	return rt.taskTypeID, true
}

// ArchetypeDisplayName returns the human-readable name for a given archetype ID.
// This method should only be used by CHASM framework internal code,
// NOT CHASM library developers.
func (r *Registry) ArchetypeDisplayName(id ArchetypeID) (string, bool) {
	rc, ok := r.ComponentByID(id)
	if !ok {
		return "", false
	}
	return rc.componentType, true
}

// ArchetypeIDOf returns the ArchetypeID for the given component Go type.
// This method should only be used by CHASM framework internal code,
// NOT CHASM library developers.
func (r *Registry) ArchetypeIDOf(componentGoType reflect.Type) (ArchetypeID, bool) {
	rc, ok := r.rcByGoType[componentGoType]
	if !ok {
		return UnspecifiedArchetypeID, false
	}
	return rc.componentID, true
}

func (r *Registry) component(fqn string) (*RegistrableComponent, bool) {
	rc, ok := r.rcByFqn[fqn]
	return rc, ok
}

func (r *Registry) task(fqn string) (*RegistrableTask, bool) {
	rt, ok := r.rtByFqn[fqn]
	return rt, ok
}

func (r *Registry) componentFor(componentInstance any) (*RegistrableComponent, bool) {
	rc, ok := r.rcByGoType[reflect.TypeOf(componentInstance)]
	return rc, ok
}

func (r *Registry) taskFor(taskInstance any) (*RegistrableTask, bool) {
	rt, ok := r.rtByGoType[reflect.TypeOf(taskInstance)]
	return rt, ok
}

func (r *Registry) componentOf(componentGoType reflect.Type) (*RegistrableComponent, bool) {
	rc, ok := r.rcByGoType[componentGoType]
	return rc, ok
}

func (r *Registry) taskOf(taskGoType reflect.Type) (*RegistrableTask, bool) {
	rt, ok := r.rtByGoType[taskGoType]
	return rt, ok
}

func (r *Registry) registerComponent(
	lib namer,
	rc *RegistrableComponent,
) error {
	if err := r.validate(rc); err != nil {
		return err
	}

	fqn, id, err := rc.registerToLibrary(lib)
	if err != nil {
		return err
	}

	if _, ok := r.rcByFqn[fqn]; ok {
		return fmt.Errorf("component %s is already registered", fqn)
	}

	if id == UnspecifiedArchetypeID {
		return fmt.Errorf("component %s maps to a reserved archetype id %d, please use a different name", fqn, UnspecifiedArchetypeID)
	}

	if existingComponent, ok := r.rcByID[id]; ok {
		return fmt.Errorf("component ID %d collision between %s and %s", id, fqn, existingComponent.fqType())
	}

	// rc.goType implements Component interface; therefore, it must be a struct.
	// This check to protect against the interface itself being registered.
	if !(rc.goType.Kind() == reflect.Struct ||
		(rc.goType.Kind() == reflect.Ptr && rc.goType.Elem().Kind() == reflect.Struct)) {
		return fmt.Errorf("component type %s must be struct or pointer to struct", rc.goType.String())
	}
	if _, ok := r.rcByGoType[rc.goType]; ok {
		return fmt.Errorf("component type %s is already registered", rc.goType.String())
	}
	r.warnUnmanagedFields(fqn, rc)

	r.rcByFqn[fqn] = rc
	r.rcByID[id] = rc
	r.rcByGoType[rc.goType] = rc
	return nil
}

func (r *Registry) validate(rc *RegistrableComponent) error {
	if err := r.validateName(rc.componentType); err != nil {
		return err
	}
	return r.validateVisibilityBusinessIDAlias(rc)
}

func (r *Registry) registerTask(
	lib namer,
	rt *RegistrableTask,
) error {
	if err := r.validateName(rt.taskType); err != nil {
		return err
	}

	fqn, id, err := rt.registerToLibrary(lib)
	if err != nil {
		return err
	}

	if _, ok := r.rtByFqn[fqn]; ok {
		return fmt.Errorf("task %s is already registered", fqn)
	}

	if existingTask, ok := r.rtByID[id]; ok {
		return fmt.Errorf("task type ID %d collision between %s and %s", id, fqn, existingTask.fqType())
	}

	if !(rt.goType.Kind() == reflect.Struct ||
		(rt.goType.Kind() == reflect.Ptr && rt.goType.Elem().Kind() == reflect.Struct)) {
		return fmt.Errorf("task type %s must be struct or pointer to struct", rt.goType.String())
	}
	if _, ok := r.rtByGoType[rt.goType]; ok {
		return fmt.Errorf("task type %s is already registered", rt.goType.String())
	}
	if !(rt.componentGoType.Kind() == reflect.Interface ||
		(rt.componentGoType.Kind() == reflect.Struct ||
			(rt.componentGoType.Kind() == reflect.Ptr && rt.componentGoType.Elem().Kind() == reflect.Struct)) &&
			rt.componentGoType.AssignableTo(reflect.TypeOf((*Component)(nil)).Elem())) {
		return fmt.Errorf("component type %s must be and interface or struct that implements Component interface", rt.componentGoType.String())
	}

	r.rtByFqn[fqn] = rt
	r.rtByID[id] = rt
	r.rtByGoType[rt.goType] = rt
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

func (r *Registry) validateVisibilityBusinessIDAlias(rc *RegistrableComponent) error {
	if !hasVisibilityField(rc.goType) {
		return nil
	}
	// Archetypes that contain a Field[*Visibility] must specify WithBusinessIDAlias.
	if !rc.hasBusinessIDAlias() {
		return fmt.Errorf("component %s has Field[*Visibility] but no businessID alias; use WithBusinessIDAlias option", rc.componentType)
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

func (r *Registry) registerNexusService(svc *nexus.Service) error {
	if _, ok := r.nexusServices[svc.Name]; ok {
		return fmt.Errorf("nexus service %s is already registered", svc.Name)
	}
	r.nexusServices[svc.Name] = svc
	return nil
}

// NexusServices returns all registered Nexus services.
func (r *Registry) NexusServices() map[string]*nexus.Service {
	// Return a copy to prevent external modification
	services := make(map[string]*nexus.Service, len(r.nexusServices))
	maps.Copy(services, r.nexusServices)
	return services
}
