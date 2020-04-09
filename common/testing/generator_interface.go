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

type (
	// Model represents a state transition graph that contains all the relationships of Vertex
	Model interface {
		AddEdge(...Edge)
		ListEdges() []Edge
	}

	// Generator generates a sequence of vertices based on the defined models
	// It must define InitialEntryVertex and ExitVertex
	// To use a generator:
	// for generator.HasNextVertex {
	//     generator.GetNextVertices
	// }
	Generator interface {
		// InitialEntryVertex is the beginning vertices of the graph
		// Only one vertex will be picked as the entry
		AddInitialEntryVertex(...Vertex)
		// ExitVertex is the terminate vertices of the graph
		AddExitVertex(...Vertex)
		// RandomEntryVertex is a random entry point which can be access at any state of the generator
		AddRandomEntryVertex(...Vertex)
		// AddModel loads model into the generator
		// AddModel can load multiple models and models will be joint if there is common vertices
		AddModel(Model)
		// HasNextVertex determines if there is more vertex to generate
		HasNextVertex() bool
		// GetNextVertices generates next vertex batch
		GetNextVertices() []Vertex
		// ListGeneratedVertices lists the pasted generated vertices
		ListGeneratedVertices() []Vertex
		// Reset cleans up all the internal states and reset to a brand new generator
		Reset()
		// DeepCopy copy a new instance of generator
		DeepCopy() Generator
		// SetBatchGenerationRule sets a function that used in GetNextVertex to return batch result
		SetBatchGenerationRule(func([]Vertex, []Vertex) bool)
		// SetVersion sets the event version
		SetVersion(int64)
		// GetVersion gets the event version
		GetVersion() int64
	}

	// Vertex represents a state in the model. A state represents a type of an Temporal event
	Vertex interface {
		// The name of the vertex. Usually, this will be the Temporal event type
		SetName(string)
		GetName() string
		//Equals(Vertex) bool
		// IsStrictOnNextVertex means if the vertex must be followed by its children
		// When IsStrictOnNextVertex set to true, it means this event can only follow by its neighbors
		SetIsStrictOnNextVertex(bool)
		IsStrictOnNextVertex() bool
		// MaxNextVertex means the max neighbors can branch out from this vertex
		SetMaxNextVertex(int)
		GetMaxNextVertex() int

		// SetVertexDataFunc sets a function to generate end vertex data
		SetDataFunc(func(...interface{}) interface{})
		GetDataFunc() func(...interface{}) interface{}
		GenerateData(...interface{}) interface{}
		GetData() interface{}
		DeepCopy() Vertex
	}

	// Edge is the connection between two vertices
	Edge interface {
		// StartVertex is the head of the connection
		SetStartVertex(Vertex)
		GetStartVertex() Vertex
		// EndVertex is the end of the connection
		SetEndVertex(Vertex)
		GetEndVertex() Vertex
		// Condition defines a function to determine if this connection is accessible
		SetCondition(func(...interface{}) bool)
		GetCondition() func(...interface{}) bool
		// Action defines function to perform when the end vertex reached
		SetAction(func())
		GetAction() func()
		DeepCopy() Edge
	}
)
