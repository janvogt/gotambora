package coding

// Represents a node in the nominal value hierarchy
type Node struct {
	id uint64 // Zero means not stored in DB.
	// valueOnly bool // Only use as value, can't carry other scales
	label    string   // display name
	children []uint64 // id's of children
	parent   uint64   // Parent node id. A zero value indicates a root node
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
