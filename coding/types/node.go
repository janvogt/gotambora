package types

import (
	"encoding/json"
)

const (
	nodeParentLink     = "parent"
	nodeChildrenLink   = "children"
	nodeReferencesLink = "references"
	nodeMetricsLink    = "metrics"
)

// Represents a node in the nominal value hierarchy
type Node struct {
	Id         Id
	Label      Label
	Parent     OptionalId
	Children   RelationToMany
	References RelationToMany
	Metrics    RelationToMany
}

type nodeMessage struct {
	Id    *Id    `json:"id"`
	Label *Label `json:"label"`
	Links
}

func (n Node) MarshalJSON() ([]byte, error) {
	mes := &nodeMessage{&n.Id, &n.Label, Links{}}
	mes.Links.AddOptional(nodeParentLink, n.Parent)
	mes.Links.AddToMany(nodeChildrenLink, []Id(n.Children))
	mes.Links.AddToMany(nodeReferencesLink, []Id(n.References))
	mes.Links.AddToMany(nodeMetricsLink, []Id(n.Metrics))
	return json.Marshal(mes)
}

func (n *Node) UnmarshalJSON(data []byte) (err error) {
	mes := &nodeMessage{&n.Id, &n.Label, Links{}}
	err = json.Unmarshal(data, mes)
	if err == nil {
		n.Parent = mes.Links.GetToOneOptional(nodeParentLink)
		n.Children = mes.Links.GetToMany(nodeChildrenLink)
		n.References = mes.Links.GetToMany(nodeReferencesLink)
		n.Metrics = mes.Links.GetToMany(nodeMetricsLink)
	}
	return
}

func (n *Node) SetId(id Id) {
	n.Id = id
}
