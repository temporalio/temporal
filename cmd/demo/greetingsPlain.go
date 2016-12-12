package main

type greeting struct {
	operations Operations
}

func (g greeting) ExecuteWorkflow() {
	greeting := g.operations.Greeting()
	name := g.operations.Name()
	g.operations.sayGreeting(greeting + " " + name)
}
