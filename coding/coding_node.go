package coding

import (
	"encoding/json"
)

// Represents a node in the nominal value hierarchy
type Node struct {
	Id       Id     // Zero means not stored in DB.
	Label    string // display name
	Parent   Id     // Parent node id. A zero value indicates a root node
	Children RelationToMany
	// scales    []Scale
}

type nodeDocument struct {
	Id     Id        `json:"id"`
	Label  string    `json:"label"`
	Parent Id        `json:"parent"`
	Links  nodeLinks `json:"links"`
}

type nodeLinks struct {
	Children RelationToMany `json:"children"`
}

func newDocumentFromNode(n *Node) *nodeDocument {
	return &nodeDocument{n.Id, n.Label, n.Parent, nodeLinks{n.Children}}
}

func (n *nodeDocument) asNode() *Node {
	return &Node{n.Id, n.Label, n.Parent, n.Links.Children}
}

func (n *Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(newDocumentFromNode(n))
}

func (n *Node) UnmarshalJSON(data []byte) (err error) {
	nd := nodeDocument{}
	err = json.Unmarshal(data, &nd)
	if err != nil {
		return
	}
	*n = *nd.asNode()
	return
}

type NodeDataSource interface {
	QueryNodes(q *NodeQuery, res chan<- *Node, abort <-chan chan<- error)
	CreateNode(n *Node) (err error)
	ReadNode(id Id) (n *Node, err error)
	UpdateNode(n *Node) (err error)
	DeleteNode(id Id) (err error)
}

type NodeQuery struct {
	Parents []Id
	Labels  []string
}

type IntervalScale struct {
	id     uint64
	label  string
	bounds struct {
		lower float64
		upper float64
	}
	unit string
}

type OrdinalScale struct {
}

type Scale interface {
}
