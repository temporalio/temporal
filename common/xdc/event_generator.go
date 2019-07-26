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

var (
	defaultBatchFunc = func(batch []Vertex) bool {
		return len(batch) == 0
	}
)

type (
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
		previousVertices    []Vertex
		leafVertices        []Vertex
		entryVertices       []Vertex
		exitVertices        map[Vertex]bool
		randomEntryVertices []Vertex
		dice                *rand.Rand
		canDoBatch          func([]Vertex) bool
		resetPoints         []ResetPoint
	}

	// ResetPoint is a mark in the generated event history that generator can be reset to
	ResetPoint struct {
		previousVertices []Vertex
		leafVertices     []Vertex
	}

	// Connection is the edge
	Connection struct {
		startVertex Vertex
		endVertex   Vertex
		condition   func() bool
		action      func()
	}

	// RevokeFunc is the condition inside connection
	RevokeFunc struct {
		methodName string
		input      []interface{}
	}
)

// NewEventGenerator initials the event generator
func NewEventGenerator() Generator {
	initialResetPoint := ResetPoint{
		previousVertices: make([]Vertex, 0),
		leafVertices:     make([]Vertex, 0),
	}
	return &EventGenerator{
		connections:         make(map[Vertex][]Edge),
		previousVertices:    make([]Vertex, 0),
		leafVertices:        make([]Vertex, 0),
		entryVertices:       make([]Vertex, 0),
		exitVertices:        make(map[Vertex]bool),
		randomEntryVertices: make([]Vertex, 0),
		dice:                rand.New(rand.NewSource(time.Now().Unix())),
		canDoBatch:          defaultBatchFunc,
		resetPoints:         []ResetPoint{initialResetPoint},
	}
}

// AddInitialEntryVertex adds the initial entry vertex. Generator will only start from one of the entry vertex
func (g *EventGenerator) AddInitialEntryVertex(entry ...Vertex) {
	g.entryVertices = append(g.entryVertices, entry...)
}

// AddExitVertex adds the terminate vertex in the generator
func (g *EventGenerator) AddExitVertex(exit ...Vertex) {
	for _, v := range exit {
		g.exitVertices[v] = true
	}
}

// AddRandomEntryVertex adds the random vertex in the generator
func (g *EventGenerator) AddRandomEntryVertex(exit ...Vertex) {
	g.randomEntryVertices = append(g.randomEntryVertices, exit...)
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

// ListGeneratedVertices returns all the generated history events
func (g EventGenerator) ListGeneratedVertices() []Vertex {
	return g.previousVertices
}

// HasNextVertex checks if there is accessible vertex
func (g *EventGenerator) HasNextVertex() bool {
	for _, prev := range g.previousVertices {
		if _, ok := g.exitVertices[prev]; ok {
			return false
		}
	}
	return len(g.leafVertices) > 0 || (len(g.previousVertices) == 0 && len(g.entryVertices) > 0)
}

// GetNextVertices generates a batch of history events happened in the same transaction
func (g *EventGenerator) GetNextVertices() []Vertex {
	if !g.HasNextVertex() {
		panic("Generator reached to a terminate state.")
	}

	batch := make([]Vertex, 0)
	for g.HasNextVertex() && g.canDoBatch(batch) {
		res := make([]Vertex, 0)
		switch {
		case len(g.previousVertices) == 0:
			// Generate for the first time, get the event candidates from entry vertex group
			res = append(res, g.getEntryVertex())
		case len(g.randomEntryVertices) > 0 && g.dice.Intn(len(g.connections)) == 0:
			// Get the event candidate from random vertex group
			res = append(res, g.getRandomVertex())
		default:
			// Get the event candidates based on context
			idx := g.getVertexIndexFromLeaf()
			res = append(res, g.randomNextVertex(idx)...)
			g.leafVertices = append(g.leafVertices[:idx], g.leafVertices[idx+1:]...)
		}
		g.leafVertices = append(g.leafVertices, res...)
		g.previousVertices = append(g.previousVertices, res...)
		batch = append(batch, res...)
	}
	// Create a reset point of each batch
	previousVerticesSnapshot := make([]Vertex, len(g.previousVertices))
	copy(previousVerticesSnapshot, g.previousVertices)
	leafVerticesSnapshot := make([]Vertex, len(g.leafVertices))
	copy(leafVerticesSnapshot, g.leafVertices)
	newResetPoint := ResetPoint{
		previousVertices: previousVerticesSnapshot,
		leafVertices:     leafVerticesSnapshot,
	}
	g.resetPoints = append(g.resetPoints, newResetPoint)
	return batch
}

// Reset reset the generator to the initial state
func (g *EventGenerator) Reset(idx int) {
	if idx >= len(g.resetPoints) {
		panic("The reset point does not exist.")
	}
	toReset := g.resetPoints[idx]
	g.previousVertices = toReset.previousVertices
	g.leafVertices = toReset.leafVertices
	g.dice = rand.New(rand.NewSource(time.Now().Unix()))
}

// ListResetPoint returns a list of available point to reset the event generator
// this will reset the previous generated event history
func (g *EventGenerator) ListResetPoint() []ResetPoint {
	return g.resetPoints
}

// SetCanDoBatchOnNextVertex sets a function to determine next generated batch of history events
func (g *EventGenerator) SetCanDoBatchOnNextVertex(canDoBatchFunc func([]Vertex) bool) {
	g.canDoBatch = canDoBatchFunc
}

func (g *EventGenerator) getEntryVertex() Vertex {
	if len(g.entryVertices) == 0 {
		panic("No possible start vertex to go to next step")
	}
	nextRange := len(g.entryVertices)
	nextIdx := g.dice.Intn(nextRange)
	vertex := g.entryVertices[nextIdx]
	return vertex
}

func (g *EventGenerator) getRandomVertex() Vertex {
	if len(g.randomEntryVertices) == 0 {
		panic("No possible vertex to go to next step")
	}
	nextRange := len(g.randomEntryVertices)
	nextIdx := g.dice.Intn(nextRange)
	vertex := g.randomEntryVertices[nextIdx]
	return vertex
}

func (g *EventGenerator) getVertexIndexFromLeaf() int {
	if len(g.leafVertices) == 0 {
		panic("No possible vertex to go to next step")
	}

	isAccessible := false
	nextRange := len(g.leafVertices)
	notAvailable := make(map[int]bool)
	var leaf Vertex
	var nextVertexIdx int
	for !isAccessible {
		nextVertexIdx = g.dice.Intn(nextRange)
		if _, ok := notAvailable[nextVertexIdx]; ok {
			continue
		}
		leaf = g.leafVertices[nextVertexIdx]
		if g.leafVertices[len(g.leafVertices)-1].IsStrictOnNextVertex() {
			nextVertexIdx = len(g.leafVertices) - 1
			leaf = g.leafVertices[nextVertexIdx]
		}
		neighbors := g.connections[leaf]
		for _, nextV := range neighbors {
			if nextV.GetCondition() == nil || nextV.GetCondition()() {
				isAccessible = true
				return nextVertexIdx
			}
		}
		if !isAccessible {
			notAvailable[nextVertexIdx] = true
			if len(notAvailable) == nextRange {
				panic("cannot find vertex to proceed")
			}
		}
	}
	return nextVertexIdx
}

func (g *EventGenerator) randomNextVertex(nextVertexIdx int) []Vertex {
	nextVertex := g.leafVertices[nextVertexIdx]
	count := g.dice.Intn(nextVertex.GetMaxNextVertex()) + 1
	neighbors := g.connections[nextVertex]
	neighborsRange := len(neighbors)
	res := make([]Vertex, 0)
	for i := 0; i < count; i++ {
		nextIdx := g.dice.Intn(neighborsRange)
		for neighbors[nextIdx].GetCondition() != nil && !neighbors[nextIdx].GetCondition()() {
			nextIdx = g.dice.Intn(neighborsRange)
		}
		newConnection := neighbors[nextIdx]
		newLeaf := newConnection.GetEndVertex()
		res = append(res, newLeaf)
		if newConnection.GetAction() != nil {
			newConnection.GetAction()()
		}
		if _, ok := g.exitVertices[newLeaf]; ok {
			res = []Vertex{newLeaf}
			return res
		}
	}

	return res
}

// NewConnection initials a new connection
func NewConnection(
	start Vertex,
	end Vertex,
) Edge {
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
func (c *Connection) SetCondition(condition func() bool) {
	c.condition = condition
}

// GetCondition returns the condition
func (c Connection) GetCondition() func() bool {
	return c.condition
}

// SetAction sets an action to perform when the end vertex hits
func (c Connection) SetAction(action func()) {
	c.action = action
}

// GetAction returns the action
func (c Connection) GetAction() func() {
	return c.action
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

// SetIsStrictOnNextVertex sets if a vertex can be added between the current vertex and its child Vertices
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
