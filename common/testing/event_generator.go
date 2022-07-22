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

package testing

import (
	"math/rand"
)

const (
	emptyCandidateIndex = -1
	defaultVersion      = int64(100)
)

var (
	defaultBatchFunc = func(batch []Vertex, history []Vertex) bool {
		return len(batch) == 0
	}
)

type (
	// HistoryEventVertex represents one type of history event
	HistoryEventVertex struct {
		name                 string
		isStrictOnNextVertex bool
		maxNextGeneration    int
		dataFunc             func(...interface{}) interface{}
		data                 interface{}
	}

	// HistoryEventModel is a graph represents relationships among history event types
	HistoryEventModel struct {
		edges []Edge
	}

	// EventGenerator is a history event generator
	// The event generator will generate next history event
	// based on the history event transition graph defined in the model
	EventGenerator struct {
		connections         map[string][]Edge
		previousVertices    []Vertex
		leafVertices        []Vertex
		entryVertices       []Vertex
		exitVertices        map[string]bool
		randomEntryVertices []Vertex
		dice                *rand.Rand
		seed                int64
		canDoBatch          func([]Vertex, []Vertex) bool
		version             int64
	}

	// HistoryEventEdge is the directional edge of two history events
	HistoryEventEdge struct {
		startVertex Vertex
		endVertex   Vertex
		condition   func(...interface{}) bool
		action      func()
	}

	// RevokeFunc is the condition inside edge
	// The function used to check if the edge is accessible at a certain state
	RevokeFunc struct {
	}
)

// NewEventGenerator initials the event generator
func NewEventGenerator(
	seed int64,
) Generator {

	return &EventGenerator{
		connections:         make(map[string][]Edge),
		previousVertices:    make([]Vertex, 0),
		leafVertices:        make([]Vertex, 0),
		entryVertices:       make([]Vertex, 0),
		exitVertices:        make(map[string]bool),
		randomEntryVertices: make([]Vertex, 0),
		dice:                rand.New(rand.NewSource(seed)),
		seed:                seed,
		canDoBatch:          defaultBatchFunc,
		version:             defaultVersion,
	}
}

// AddInitialEntryVertex adds the initial history event vertices
// Generator will only start from one of the entry vertex
func (g *EventGenerator) AddInitialEntryVertex(
	entry ...Vertex,
) {

	g.entryVertices = append(
		g.entryVertices,
		entry...)
}

// AddExitVertex adds the terminate history event vertex
func (g *EventGenerator) AddExitVertex(
	exit ...Vertex,
) {

	for _, v := range exit {
		g.exitVertices[v.GetName()] = true
	}
}

// AddRandomEntryVertex adds the random history event vertex
func (g *EventGenerator) AddRandomEntryVertex(
	exit ...Vertex,
) {

	g.randomEntryVertices = append(g.randomEntryVertices, exit...)
}

// AddModel adds a history event model
func (g *EventGenerator) AddModel(
	model Model,
) {

	for _, e := range model.ListEdges() {
		if _, ok := g.connections[e.GetStartVertex().GetName()]; !ok {
			g.connections[e.GetStartVertex().GetName()] = make([]Edge, 0)
		}
		g.connections[e.GetStartVertex().GetName()] = append(g.connections[e.GetStartVertex().GetName()], e)
	}
}

// ListGeneratedVertices returns all the generated history events
func (g EventGenerator) ListGeneratedVertices() []Vertex {

	return g.previousVertices
}

// HasNextVertex checks if there is accessible history event vertex
func (g *EventGenerator) HasNextVertex() bool {

	for _, prev := range g.previousVertices {
		if _, ok := g.exitVertices[prev.GetName()]; ok {
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
	for g.HasNextVertex() && g.canDoBatch(batch, g.previousVertices) {
		res := g.generateNextEventBatch()
		g.updateContext(res)
		batch = append(batch, res...)
	}
	return batch
}

// Reset reset the generator to the initial state
func (g *EventGenerator) Reset() {

	g.leafVertices = make([]Vertex, 0)
	g.previousVertices = make([]Vertex, 0)
	g.version = defaultVersion
}

// DeepCopy copy a new instance of generator
func (g *EventGenerator) DeepCopy() Generator {

	return &EventGenerator{
		connections:         copyConnections(g.connections),
		previousVertices:    copyVertex(g.previousVertices),
		leafVertices:        copyVertex(g.leafVertices),
		entryVertices:       copyVertex(g.entryVertices),
		exitVertices:        copyExitVertices(g.exitVertices),
		randomEntryVertices: copyVertex(g.randomEntryVertices),
		dice:                rand.New(rand.NewSource(g.seed)),
		seed:                g.seed,
		canDoBatch:          g.canDoBatch,
		version:             g.version,
	}
}

// SetBatchGenerationRule sets a function to determine next generated batch of history events
func (g *EventGenerator) SetBatchGenerationRule(
	canDoBatchFunc func([]Vertex, []Vertex) bool,
) {

	g.canDoBatch = canDoBatchFunc
}

// SetVersion sets the event version
func (g *EventGenerator) SetVersion(version int64) {

	g.version = version
}

// GetVersion returns event version
func (g *EventGenerator) GetVersion() int64 {

	return g.version
}

func (g *EventGenerator) generateNextEventBatch() []Vertex {

	batch := make([]Vertex, 0)
	switch {
	case len(g.previousVertices) == 0:
		// Generate for the first time, get the event candidates from entry vertex group
		batch = append(batch, g.getEntryVertex())
	case len(g.randomEntryVertices) > 0 && g.dice.Intn(len(g.connections)) == 0:
		// Get the event candidate from random vertex group
		batch = append(batch, g.getRandomVertex())
	default:
		// Get the event candidates based on context
		idx := g.getVertexCandidate()
		batch = append(batch, g.randomNextVertex(idx)...)
		g.leafVertices = append(g.leafVertices[:idx], g.leafVertices[idx+1:]...)
	}
	return batch
}

func (g *EventGenerator) updateContext(
	batch []Vertex,
) {

	g.leafVertices = append(g.leafVertices, batch...)
	g.previousVertices = append(g.previousVertices, batch...)
}

func (g *EventGenerator) getEntryVertex() Vertex {

	if len(g.entryVertices) == 0 {
		panic("No possible start vertex to go to next step")
	}
	nextRange := len(g.entryVertices)
	nextIdx := g.dice.Intn(nextRange)
	vertex := g.entryVertices[nextIdx].DeepCopy()
	vertex.GenerateData(nil)
	return vertex
}

func (g *EventGenerator) getRandomVertex() Vertex {

	if len(g.randomEntryVertices) == 0 {
		panic("No possible vertex to go to next step")
	}
	nextRange := len(g.randomEntryVertices)
	nextIdx := g.dice.Intn(nextRange)
	vertex := g.randomEntryVertices[nextIdx].DeepCopy()
	parentEvent := g.previousVertices[len(g.previousVertices)-1]

	vertex.GenerateData(parentEvent.GetData(), parentEvent.GetData(), g.version)
	return vertex
}

func (g *EventGenerator) getVertexCandidate() int {

	if len(g.leafVertices) == 0 {
		panic("No possible vertex to go to next step")
	}
	nextRange := len(g.leafVertices)
	notAvailable := make(map[int]bool)
	var nextVertexIdx int
	for len(notAvailable) < nextRange {
		nextVertexIdx = g.dice.Intn(nextRange)
		// If the vertex is not accessible at this state, skip it
		if _, ok := notAvailable[nextVertexIdx]; ok {
			continue
		}
		isAccessible, nextVertexIdx := g.findAccessibleVertex(nextVertexIdx)
		if isAccessible {
			return nextVertexIdx
		}
		notAvailable[nextVertexIdx] = true
	}
	// If all history event cannot be accessible, which means the model is incorrect
	panic("cannot find available history event to proceed. please check your model")
}

func (g *EventGenerator) findAccessibleVertex(
	vertexIndex int,
) (bool, int) {

	candidate := g.leafVertices[vertexIndex]
	if g.leafVertices[len(g.leafVertices)-1].IsStrictOnNextVertex() {
		vertexIndex = len(g.leafVertices) - 1
		candidate = g.leafVertices[vertexIndex]
	}
	neighbors := g.connections[candidate.GetName()]
	for _, nextV := range neighbors {
		if nextV.GetCondition() == nil || nextV.GetCondition()(g.previousVertices) {
			return true, vertexIndex
		}
	}
	return false, emptyCandidateIndex
}

func (g *EventGenerator) randomNextVertex(
	nextVertexIdx int,
) []Vertex {

	nextVertex := g.leafVertices[nextVertexIdx]

	count := g.dice.Intn(nextVertex.GetMaxNextVertex()) + 1
	res := make([]Vertex, 0)
	latestVertex := g.previousVertices[len(g.previousVertices)-1]
	for i := 0; i < count; i++ {
		endVertex := g.pickRandomVertex(nextVertex)
		endVertex.GenerateData(nextVertex.GetData(), latestVertex.GetData(), g.version)
		latestVertex = endVertex
		res = append(res, endVertex)
		if _, ok := g.exitVertices[endVertex.GetName()]; ok {
			res = []Vertex{endVertex}
			return res
		}
	}
	return res
}

func (g *EventGenerator) pickRandomVertex(
	nextVertex Vertex,
) Vertex {

	neighbors := g.connections[nextVertex.GetName()]
	neighborsRange := len(neighbors)
	nextIdx := g.dice.Intn(neighborsRange)
	for neighbors[nextIdx].GetCondition() != nil && !neighbors[nextIdx].GetCondition()(g.previousVertices) {
		nextIdx = g.dice.Intn(neighborsRange)
	}
	newConnection := neighbors[nextIdx]
	endVertex := newConnection.GetEndVertex()
	if newConnection.GetAction() != nil {
		newConnection.GetAction()()
	}
	return endVertex.DeepCopy()
}

// NewHistoryEventEdge initials a new edge between two HistoryEventVertexes
func NewHistoryEventEdge(
	start Vertex,
	end Vertex,
) Edge {

	return &HistoryEventEdge{
		startVertex: start,
		endVertex:   end,
	}
}

// SetStartVertex sets the start vertex
func (c *HistoryEventEdge) SetStartVertex(
	start Vertex,
) {

	c.startVertex = start
}

// GetStartVertex returns the start vertex
func (c HistoryEventEdge) GetStartVertex() Vertex {

	return c.startVertex
}

// SetEndVertex sets the end vertex
func (c *HistoryEventEdge) SetEndVertex(end Vertex) {

	c.endVertex = end
}

// GetEndVertex returns the end vertex
func (c HistoryEventEdge) GetEndVertex() Vertex {

	return c.endVertex
}

// SetCondition sets the condition to access this edge
func (c *HistoryEventEdge) SetCondition(
	condition func(...interface{}) bool,
) {

	c.condition = condition
}

// GetCondition returns the condition
func (c HistoryEventEdge) GetCondition() func(...interface{}) bool {

	return c.condition
}

// SetAction sets an action to perform when the end vertex hits
func (c *HistoryEventEdge) SetAction(action func()) {

	c.action = action
}

// GetAction returns the action
func (c HistoryEventEdge) GetAction() func() {

	return c.action
}

// DeepCopy copies a new edge
func (c *HistoryEventEdge) DeepCopy() Edge {

	return &HistoryEventEdge{
		startVertex: c.startVertex.DeepCopy(),
		endVertex:   c.endVertex.DeepCopy(),
		condition:   c.condition,
		action:      c.action,
	}
}

// NewHistoryEventVertex initials a history event vertex
func NewHistoryEventVertex(
	name string,
) Vertex {

	return &HistoryEventVertex{
		name:                 name,
		isStrictOnNextVertex: false,
		maxNextGeneration:    1,
	}
}

// GetName returns the name
func (he HistoryEventVertex) GetName() string {

	return he.name
}

// SetName sets the name
func (he *HistoryEventVertex) SetName(
	name string,
) {

	he.name = name
}

// Equals compares two vertex
// func (he *HistoryEventVertex) Equals(
//	v Vertex,
// ) bool {
//
//	return strings.EqualFold(he.name, v.GetName()) && he.data == v.GetData()
// }

// SetIsStrictOnNextVertex sets if a vertex can be added between the current vertex and its child Vertices
func (he *HistoryEventVertex) SetIsStrictOnNextVertex(
	isStrict bool,
) {

	he.isStrictOnNextVertex = isStrict
}

// IsStrictOnNextVertex returns the isStrict flag
func (he HistoryEventVertex) IsStrictOnNextVertex() bool {

	return he.isStrictOnNextVertex
}

// SetMaxNextVertex sets the max concurrent path can be generated from this vertex
func (he *HistoryEventVertex) SetMaxNextVertex(
	maxNextGeneration int,
) {

	if maxNextGeneration < 1 {
		panic("max next vertex number cannot less than 1")
	}
	he.maxNextGeneration = maxNextGeneration
}

// GetMaxNextVertex returns the max concurrent path
func (he HistoryEventVertex) GetMaxNextVertex() int {

	return he.maxNextGeneration
}

// SetDataFunc sets the data generation function
func (he *HistoryEventVertex) SetDataFunc(
	dataFunc func(...interface{}) interface{},
) {

	he.dataFunc = dataFunc
}

// GetDataFunc returns the data generation function
func (he HistoryEventVertex) GetDataFunc() func(...interface{}) interface{} {

	return he.dataFunc
}

// GenerateData generates the data and return
func (he *HistoryEventVertex) GenerateData(
	input ...interface{},
) interface{} {

	if he.dataFunc == nil {
		return nil
	}

	he.data = he.dataFunc(input...)
	return he.data
}

// GetData returns the vertex data
func (he HistoryEventVertex) GetData() interface{} {

	return he.data
}

// DeepCopy returns the a deep copy of vertex
func (he HistoryEventVertex) DeepCopy() Vertex {

	return &HistoryEventVertex{
		name:                 he.GetName(),
		isStrictOnNextVertex: he.IsStrictOnNextVertex(),
		maxNextGeneration:    he.GetMaxNextVertex(),
		dataFunc:             he.GetDataFunc(),
		data:                 he.GetData(),
	}
}

// NewHistoryEventModel initials new history event model
func NewHistoryEventModel() Model {

	return &HistoryEventModel{
		edges: make([]Edge, 0),
	}
}

// AddEdge adds an edge to the model
func (m *HistoryEventModel) AddEdge(
	edge ...Edge,
) {

	m.edges = append(m.edges, edge...)
}

// ListEdges returns all added edges
func (m HistoryEventModel) ListEdges() []Edge {

	return m.edges
}
