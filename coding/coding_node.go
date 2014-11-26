package coding

// Represents a node in the nominal value hierarchy
type Node struct {
	Id uint64 `json:id` // Zero means not stored in DB.
	// valueOnly bool // Only use as value, can't carry other scales
	Label    string   `json:label`    // display name
	Children []uint64 `json:children` // id's of children
	Parent   uint64   `json:parent`   // Parent node id. A zero value indicates a root node
	// scales    []Scale
}

func (n *Node) Save() {
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
