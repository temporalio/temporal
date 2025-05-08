package chasm

func (r *Registry) Component(fqn string) (*RegistrableComponent, bool) {
	return r.component(fqn)
}

func (r *Registry) Task(fqn string) (*RegistrableTask, bool) {
	return r.task(fqn)
}

func (r *Registry) ComponentFor(componentInstance any) (*RegistrableComponent, bool) {
	return r.componentFor(componentInstance)
}

func (r *Registry) TaskFor(taskInstance any) (*RegistrableTask, bool) {
	return r.taskFor(taskInstance)
}

func (rc RegistrableComponent) FqType() string {
	return rc.fqType()
}

func (rt RegistrableTask) FqType() string {
	return rt.fqType()
}
