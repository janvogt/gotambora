package types

import (
	"encoding/json"
)

// Represents a node in the nominal value hierarchy
type Node struct {
	Id       Id    // Zero means not stored in DB.
	Label    Label // display name
	Parent   Id    // Parent node id. A zero value indicates a root node
	Children RelationToMany
	// scales    []Scale
}

type nodeDocument struct {
	Id     Id        `json:"id"`
	Label  Label     `json:"label"`
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

func (n *Node) SetId(id Id) {
	n.Id = id
}

type NodeDataSource interface {
	QueryNodes(q *NodeQuery, res chan<- *Node, abort <-chan chan<- error)
	CreateNode(n *Node) (err error)
	ReadNode(id Id) (n *Node, err error)
	UpdateNode(n *Node) (err error)
	DeleteNode(id Id) (err error)
}

// CategoryReader is a source for a set of categories.
type NodeReader interface {
	Read(n *Node) (ok bool, err error) // Reads the next Category in the CategoryResult into c. If sucessful ok is true, if no catagory was read ok is false, if there was an error it is returned.
}

type NodeQuery struct {
	Parents []Id
	Labels  []Label
}
