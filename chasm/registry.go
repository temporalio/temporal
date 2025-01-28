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
		components     map[string]RegistrableComponent // fully qualified name -> component
		componentNames map[reflect.Type]string         // component type -> fully qualified name

		tasks     map[string]RegistrableTask // fully qualified name -> task
		taskNames map[reflect.Type]string    // task type -> fully qualified name
	}
)

func NewRegistry() *Registry {
	return &Registry{
		components:     make(map[string]RegistrableComponent),
		componentNames: make(map[reflect.Type]string),
		tasks:          make(map[string]RegistrableTask),
		taskNames:      make(map[reflect.Type]string),
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

func (r *Registry) Component(fqn string) (RegistrableComponent, bool) {
	rc, ok := r.components[fqn]
	if !ok {
		return EmptyRegistrableComponent, false
	}
	return rc, ok
}

func (r *Registry) Task(fqn string) (RegistrableTask, bool) {
	rt, ok := r.tasks[fqn]
	if !ok {
		return EmptyRegistrableTask, false
	}
	return rt, ok
}

func (r *Registry) ComponentFor(componentInstance any) (RegistrableComponent, bool) {
	fqn, ok := r.componentNames[reflect.TypeOf(componentInstance)]
	if !ok {
		return EmptyRegistrableComponent, false
	}
	return r.Component(fqn)
}

func (r *Registry) TaskFor(taskInstance any) (RegistrableTask, bool) {
	fqn, ok := r.taskNames[reflect.TypeOf(taskInstance)]
	if !ok {
		return EmptyRegistrableTask, false
	}
	return r.Task(fqn)
}

func (r *Registry) fqn(libName, name string) string {
	return libName + "." + name
}

func (r *Registry) registerComponent(
	libName string,
	c RegistrableComponent,
) error {
	if err := r.validateName(c.name); err != nil {
		return err
	}
	fqn := r.fqn(libName, c.name)
	if _, ok := r.components[fqn]; ok {
		return fmt.Errorf("component %s is already registered", fqn)
	}
	// TODO: this step is redundant. c.goType implements Component interface, therefore it must be a struct.
	if !(c.goType.Kind() == reflect.Struct ||
		(c.goType.Kind() == reflect.Ptr && c.goType.Elem().Kind() == reflect.Struct)) {
		return fmt.Errorf("component type %s must be struct or pointer to struct", c.goType.String())
	}
	if _, ok := r.componentNames[c.goType]; ok {
		return fmt.Errorf("component type %s is already registered", c.goType.String())
	}
	r.components[fqn] = c
	r.componentNames[c.goType] = fqn
	return nil
}
func (r *Registry) registerTask(
	libName string,
	t RegistrableTask,
) error {
	if err := r.validateName(t.name); err != nil {
		return err
	}
	fqn := r.fqn(libName, t.name)
	if _, ok := r.tasks[fqn]; ok {
		return fmt.Errorf("task %s is already registered", fqn)
	}
	if !(t.goType.Kind() == reflect.Struct ||
		(t.goType.Kind() == reflect.Ptr && t.goType.Elem().Kind() == reflect.Struct)) {
		return fmt.Errorf("task type %s must be struct or pointer to struct", t.goType.String())
	}
	if _, ok := r.taskNames[t.goType]; ok {
		return fmt.Errorf("task type %s is already registered", t.goType.String())
	}
	if !(t.componentGoType.Kind() == reflect.Interface ||
		(t.componentGoType.Kind() == reflect.Struct ||
			(t.componentGoType.Kind() == reflect.Ptr && t.componentGoType.Elem().Kind() == reflect.Struct)) &&
			t.componentGoType.AssignableTo(reflect.TypeOf((*Component)(nil)).Elem())) {
		return fmt.Errorf("component type %s must be and interface or struct that implements Component interface", t.componentGoType.String())
	}

	r.tasks[fqn] = t
	r.taskNames[t.goType] = fqn
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
