// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package schema

import (
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/path"
	"gonum.org/v1/gonum/graph/simple"
)

// versionNode is the version representation in the schema graph
// implements gonum.org/v1/gonum/graph Node
type versionNode struct {
	id    int64
	value string
}

func (n *versionNode) ID() int64 {
	return n.id
}

// versionEdge is the schema update representation in the schema graph
// implements gonum.org/v1/gonum/graph Edge
type versionEdge struct {
	from    graph.Node
	to      graph.Node
	dirName string
}

func (e *versionEdge) From() graph.Node {
	return e.from
}

func (e *versionEdge) To() graph.Node {
	return e.to
}

func (e *versionEdge) ReversedEdge() graph.Edge {
	return &versionEdge{to: e.from, from: e.to, dirName: e.dirName}
}

func addNode(version string, g graph.NodeAdder, m map[string]*versionNode) graph.Node {
	n := &versionNode{id: int64(len(m)), value: version}
	m[version] = n
	g.AddNode(n)
	return n
}

func findShortestPath(startVer string, endVer string, increments []string, shortcuts []squashVersion) ([]string, error) {
	nodeMap := make(map[string]*versionNode, len(increments)+1)
	g := simple.NewDirectedGraph()
	startNode := addNode(startVer, g, nodeMap)
	endNode := addNode(endVer, g, nodeMap)
	prev := startNode
	for _, inc := range increments {
		next := endNode
		v := dirToVersion(inc)
		if v != endVer {
			next = addNode(v, g, nodeMap)
		}
		e := &versionEdge{from: prev, to: next, dirName: inc}
		g.SetEdge(e)
		prev = next
	}

	for _, s := range shortcuts {
		l, lok := nodeMap[s.prev]
		r, rok := nodeMap[s.ver]
		if lok && rok {
			e := &versionEdge{from: l, to: r, dirName: s.dirName}
			g.SetEdge(e)
		}
	}

	var res []string
	p, _ := path.DijkstraFrom(startNode, g).To(endNode.ID())
	for i := range p {
		// given the path, populate the directory slice
		if i == 0 {
			continue
		}
		res = append(res, g.Edge(p[i-1].ID(), p[i].ID()).(*versionEdge).dirName)
	}

	return res, nil
}
