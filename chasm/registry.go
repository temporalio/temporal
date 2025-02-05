// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package chasm

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
)

var (
	// This is golang type identifier regex.
	nameValidator = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type (
	Registry struct {
		componentByType   map[string]*RegistrableComponent       // fully qualified type name -> component
		componentByGoType map[reflect.Type]*RegistrableComponent // component go type -> component

		taskByType   map[string]*RegistrableTask       // fully qualified type name -> task
		taskByGoType map[reflect.Type]*RegistrableTask // task go type -> task
	}
)

func NewRegistry() *Registry {
	return &Registry{
		componentByType:   make(map[string]*RegistrableComponent),
		componentByGoType: make(map[reflect.Type]*RegistrableComponent),
		taskByType:        make(map[string]*RegistrableTask),
		taskByGoType:      make(map[reflect.Type]*RegistrableTask),
	}
}

func (r *Registry) Register(lib Library) error {
	if err := r.validateName(lib.Name()); err != nil {
		return err
	}
	for _, c := range lib.Components() {
		if err := r.registerComponent(lib.Name(), c); err != nil {
			return err
		}
	}
	for _, t := range lib.Tasks() {
		if err := r.registerTask(lib.Name(), t); err != nil {
			return err
		}
	}
	return nil
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

func (r *Registry) fqn(libName, name string) string {
	return libName + "." + name
}

func (r *Registry) registerComponent(
	libName string,
	rc *RegistrableComponent,
) error {
	if err := r.validateName(rc.componentType); err != nil {
		return err
	}
	fqn := r.fqn(libName, rc.componentType)
	if _, ok := r.componentByType[fqn]; ok {
		return fmt.Errorf("component %s is already registered", fqn)
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
	r.componentByType[fqn] = rc
	r.componentByGoType[rc.goType] = rc
	return nil
}
func (r *Registry) registerTask(
	libName string,
	rt *RegistrableTask,
) error {
	if err := r.validateName(rt.taskType); err != nil {
		return err
	}
	fqn := r.fqn(libName, rt.taskType)
	if _, ok := r.taskByType[fqn]; ok {
		return fmt.Errorf("task %s is already registered", fqn)
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
