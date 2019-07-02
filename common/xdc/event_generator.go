// Copyright (c) 2019 Uber Technologies, Inc.
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

package xdc

import (
	"math/rand"
	"strings"
	"time"
)

type (
	// Model represents a state transition graph that contains all the relationships of Vertex
	Model interface {
		AddEdge(...Edge)
		ListEdges() []Edge
	}

	// Generator generates a sequence of Vertexes based on the defined models
	// It must define InitialEntryVertex and ExitVertex
	// RandomEntryVertex is a random entry point which can be access at any state of the generator
	Generator interface {
		AddInitialEntryVertex(...Vertex)
		AddExitVertex(...Vertex)
		AddRandomEntryVertex(...Vertex)
		AddModel(Model)
		HasNextVertex() bool
		GetNextVertex() []Vertex
		ListGeneratedVertex() []Vertex
		Reset()
	}

	// Vertex represents a state in the model. A state can be an event
	// IsStrictOnNextVertex means if the vertex must be followed by its children
	// MaxNextVertex means the max concurrent path can branch out from this vertex
	Vertex interface {
		SetName(string)
		GetName() string
		Equals(Vertex) bool
		SetIsStrictOnNextVertex(bool)
		IsStrictOnNextVertex() bool
		SetMaxNextVertex(int)
		GetMaxNextVertex() int
	}

	// Edge is the relationship between two vertexes
	// The condition means if the edge is accessible at the state
	Edge interface {
		SetStartVertex(Vertex)
		GetStartVertex() Vertex
		SetEndVertex(Vertex)
		GetEndVertex() Vertex
		SetCondition(func(vertexes []Vertex) bool)
		GetCondition() func(vertexes []Vertex) bool
	}

	// HistoryEvent is the history event vertex
	HistoryEvent struct {
		name                 string
		isStrictOnNextVertex bool
		maxNextGeneration    int
	}

	// HistoryEventModel is the history event model
	HistoryEventModel struct {
		edges []Edge
	}

	// EventGenerator is a event generator
	EventGenerator struct {
		connections         map[Vertex][]Edge
		previousVertexes    []Vertex
		leafVertexes        []Vertex
		entryVertexes       []Vertex
		exitVertexes        map[Vertex]bool
		randomEntryVertexes []Vertex
		dice                *rand.Rand
	}

	// Connection is the edge
	Connection struct {
		startVertex Vertex
		endVertex   Vertex
		condition   func(vertexes []Vertex) bool
	}

	// RevokeFunc is the condition inside connection
	RevokeFunc struct {
		methodName string
		input      []interface{}
	}
)

// NewEventGenerator initials the event generator
func NewEventGenerator() Generator {
	return &EventGenerator{
		connections:         make(map[Vertex][]Edge),
		previousVertexes:    make([]Vertex, 0),
		leafVertexes:        make([]Vertex, 0),
		entryVertexes:       make([]Vertex, 0),
		exitVertexes:        make(map[Vertex]bool),
		randomEntryVertexes: make([]Vertex, 0),
		dice:                rand.New(rand.NewSource(time.Now().Unix())),
	}
}

// AddInitialEntryVertex adds the initial entry vertex. Generator will only start from one of the entry vertex
func (g *EventGenerator) AddInitialEntryVertex(entry ...Vertex) {
	g.entryVertexes = append(g.entryVertexes, entry...)
}

// AddExitVertex adds the terminate vertex in the generator
func (g *EventGenerator) AddExitVertex(exit ...Vertex) {
	for _, v := range exit {
		g.exitVertexes[v] = true
	}
}

// AddRandomEntryVertex adds the random vertex in the generator
func (g *EventGenerator) AddRandomEntryVertex(exit ...Vertex) {
	g.randomEntryVertexes = append(g.randomEntryVertexes, exit...)
}

// AddModel adds a model
func (g *EventGenerator) AddModel(model Model) {
	for _, e := range model.ListEdges() {
		if _, ok := g.connections[e.GetStartVertex()]; !ok {
			g.connections[e.GetStartVertex()] = make([]Edge, 0)
		}
		g.connections[e.GetStartVertex()] = append(g.connections[e.GetStartVertex()], e)
	}
}

// ListGeneratedVertex returns all the generated vertex
func (g EventGenerator) ListGeneratedVertex() []Vertex {
	return g.previousVertexes
}

// HasNextVertex checks if there is accessible vertex
func (g *EventGenerator) HasNextVertex() bool {
	for _, prev := range g.previousVertexes {
		if _, ok := g.exitVertexes[prev]; ok {
			return false
		}
	}
	return len(g.leafVertexes) > 0 || (len(g.previousVertexes) == 0 && len(g.entryVertexes) > 0)
}

// GetNextVertex generates the next vertex
func (g *EventGenerator) GetNextVertex() []Vertex {
	if !g.HasNextVertex() {
		panic("Generator reached to a terminate state.")
	}

	//Hardcoded to see get next vertex from pool or random entry vertex
	res := make([]Vertex, 0)
	switch {
	case len(g.previousVertexes) == 0:
		res = append(res, g.randomEntryVertex())
	case g.previousVertexes[len(g.previousVertexes)-1].IsStrictOnNextVertex() || g.dice.Intn(len(g.connections)) != 0:
		vertex := g.getNextPossibleVertex()
		res = append(res, g.randomNextPossibleVertex(vertex)...)
	case len(g.randomEntryVertexes) > 0:
		res = append(res, g.randomRandomVertex())
	default:
		vertex := g.getNextPossibleVertex()
		res = append(res, g.randomNextPossibleVertex(vertex)...)
	}
	return res
}

// Reset reset the generator to the initial state
func (g *EventGenerator) Reset() {
	g.previousVertexes = make([]Vertex, 0)
	g.leafVertexes = make([]Vertex, 0)
	g.dice = rand.New(rand.NewSource(time.Now().Unix()))
}

func (g *EventGenerator) randomEntryVertex() Vertex {
	if len(g.entryVertexes) == 0 {
		panic("No possible start vertex to go to next step")
	}
	nextRange := len(g.entryVertexes)
	nextIdx := g.dice.Intn(nextRange)
	vertex := g.entryVertexes[nextIdx]
	g.leafVertexes = append(g.leafVertexes, vertex)
	g.previousVertexes = append(g.previousVertexes, vertex)
	return vertex
}

func (g *EventGenerator) randomRandomVertex() Vertex {
	if len(g.randomEntryVertexes) == 0 {
		panic("No possible vertex to go to next step")
	}
	nextRange := len(g.randomEntryVertexes)
	nextIdx := g.dice.Intn(nextRange)
	vertex := g.randomEntryVertexes[nextIdx]
	g.leafVertexes = append(g.leafVertexes, vertex)
	g.previousVertexes = append(g.previousVertexes, vertex)
	return vertex
}

func (g *EventGenerator) getNextPossibleVertex() Vertex {
	if len(g.leafVertexes) == 0 {
		panic("No possible vertex to go to next step")
	}

	isAccessible := false
	nextRange := len(g.leafVertexes)
	notAvailable := make(map[int]bool)
	var leaf Vertex
	var nextVertexIdx int
	for !isAccessible {
		nextVertexIdx = g.dice.Intn(nextRange)
		if _, ok := notAvailable[nextVertexIdx]; ok {
			continue
		}
		leaf = g.leafVertexes[nextVertexIdx]
		if g.leafVertexes[len(g.leafVertexes)-1].IsStrictOnNextVertex() {
			nextVertexIdx = len(g.leafVertexes) - 1
			leaf = g.leafVertexes[nextVertexIdx]
		}
		neighbors := g.connections[leaf]
		for _, nextV := range neighbors {
			if nextV.GetCondition() == nil || (nextV.GetCondition() != nil && nextV.GetCondition()(g.previousVertexes)) {
				isAccessible = true
				break
			}
		}
		if !isAccessible {
			notAvailable[nextVertexIdx] = true
			if len(notAvailable) == nextRange {
				panic("cannot find vertex to proceed")
			}
		}
	}
	g.leafVertexes = append(g.leafVertexes[:nextVertexIdx], g.leafVertexes[nextVertexIdx+1:]...)
	return leaf
}

func (g *EventGenerator) randomNextPossibleVertex(next Vertex) []Vertex {
	count := g.dice.Intn(next.GetMaxNextVertex()) + 1
	neighbors := g.connections[next]
	neighborsRange := len(neighbors)
	res := make([]Vertex, 0)
	for i := 0; i < count; i++ {
		nextIdx := g.dice.Intn(neighborsRange)
		for neighbors[nextIdx].GetCondition() != nil && !neighbors[nextIdx].GetCondition()(g.previousVertexes) {
			nextIdx = g.dice.Intn(neighborsRange)
		}
		newLeaf := neighbors[nextIdx].GetEndVertex()
		g.previousVertexes = append(g.previousVertexes, newLeaf)
		g.leafVertexes = append(g.leafVertexes, newLeaf)
		res = append(res, newLeaf)
		if _, ok := g.exitVertexes[newLeaf]; ok {
			break
		}
	}
	return res
}

// NewConnection initials a new connection
func NewConnection(start Vertex, end Vertex) Edge {
	return &Connection{
		startVertex: start,
		endVertex:   end,
	}
}

// SetStartVertex sets the start vertex
func (c *Connection) SetStartVertex(start Vertex) {
	c.startVertex = start
}

// GetStartVertex returns the start vertex
func (c Connection) GetStartVertex() Vertex {
	return c.startVertex
}

// SetEndVertex sets the end vertex
func (c *Connection) SetEndVertex(end Vertex) {
	c.endVertex = end
}

// GetEndVertex returns the end vertex
func (c Connection) GetEndVertex() Vertex {
	return c.endVertex
}

// SetCondition sets the condition to access this edge
func (c *Connection) SetCondition(condition func(vertexes []Vertex) bool) {
	c.condition = condition
}

// GetCondition returns the condition
func (c Connection) GetCondition() func(vertexes []Vertex) bool {
	return c.condition
}

// NewHistoryEvent initials a history event
func NewHistoryEvent(name string) Vertex {
	return &HistoryEvent{
		name:                 name,
		isStrictOnNextVertex: false,
		maxNextGeneration:    1,
	}
}

// GetName returns the name
func (he HistoryEvent) GetName() string {
	return he.name
}

// SetName sets the name
func (he *HistoryEvent) SetName(name string) {
	he.name = name
}

// Equals compares two vertex
func (he *HistoryEvent) Equals(v Vertex) bool {
	return strings.EqualFold(he.name, v.GetName())
}

// SetIsStrictOnNextVertex sets if a vertex can be added between the current vertex and its child vertexes
func (he *HistoryEvent) SetIsStrictOnNextVertex(isStrict bool) {
	he.isStrictOnNextVertex = isStrict
}

// IsStrictOnNextVertex returns the isStrict flag
func (he HistoryEvent) IsStrictOnNextVertex() bool {
	return he.isStrictOnNextVertex
}

// SetMaxNextVertex sets the max concurrent path can be generated from this vertex
func (he *HistoryEvent) SetMaxNextVertex(maxNextGeneration int) {
	if maxNextGeneration < 1 {
		panic("max next vertex number cannot less than 1")
	}
	he.maxNextGeneration = maxNextGeneration
}

// GetMaxNextVertex returns the max concurrent path
func (he HistoryEvent) GetMaxNextVertex() int {
	return he.maxNextGeneration
}

// NewHistoryEventModel initials new history event model
func NewHistoryEventModel() Model {
	return &HistoryEventModel{
		edges: make([]Edge, 0),
	}
}

// AddEdge adds an edge to the model
func (m *HistoryEventModel) AddEdge(edge ...Edge) {
	m.edges = append(m.edges, edge...)
}

// ListEdges returns all added edges
func (m HistoryEventModel) ListEdges() []Edge {
	return m.edges
}
