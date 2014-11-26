package coding

// Represents a node in the nominal value hierarchy
type Node struct {
	Id uint64 // Zero means not stored in DB.
	// valueOnly bool // Only use as value, can't carry other scales
	Label    string   // display name
	Children []uint64 // id's of children
	Parent   uint64   // Parent node id. A zero value indicates a root node
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
