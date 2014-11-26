package coding

import (
	"errors"
)

// Represents a node in the nominal value hierarchy
type Node struct {
	Id uint64 `json:id` // Zero means not stored in DB.
	// valueOnly bool // Only use as value, can't carry other scales
	Label  string `json:label`  // display name
	Parent uint64 `json:parent` // Parent node id. A zero value indicates a root node
	// scales    []Scale
	ds DataSource // Pointer to datasource this node belongs to.
}

var (
	ErrNewNode = errors.New("Node has not been stored yet.")
)

func (n *Node) Save() (err error) {
	var res *Node
	if n.IsNew() {
		res, err = n.ds.InsertNode(n)
	} else {
		res, err = n.ds.UpdateNode(n)
	}
	if err == nil {
		*n = *res
	}
	return
}

func (n *Node) Load() (err error) {
	if n.IsNew() {
		err = ErrNewNode
		return
	}
	res, err := n.ds.QueryNodes(n.Id, 0)
	if err == nil {
		*n = res[0]
	}
	return
}

func (n *Node) Delete() (err error) {
	if n.IsNew() {
		err = ErrNewNode
		return
	}
	err = n.ds.DeleteNode(n.Id)
	if err == nil {
		n.Id = 0
	}
	return
}

func (n *Node) IsNew() (isNew bool) {
	return n.Id == 0
}

func RootNodes(d DataSource) ([]Node, error) {
	return ChildNodes(d, 0)
}

func NewNode(d DataSource) *Node {
	return &Node{ds: d}
}

func ChildNodes(d DataSource, parent uint64) ([]Node, error) {
	nodes, err := d.QueryNodes(0, parent)
	if nodes != nil {
		for i := range nodes {
			nodes[i].ds = d
		}
	} else {
		nodes = make([]Node, 0)
	}
	return nodes, err
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
