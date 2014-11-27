package coding

import (
	"errors"
	"fmt"
	"testing"
)

// TestDb is an implementation of a DataSource for testing.
type TestDb struct {
	calls    []string
	retNodes map[string][]Node
	retNode  map[string]*Node
	err      map[string]error
}

// QueryNodes stores it's call and returns an error if availiable and a slice of nodes otherwise.
func (db *TestDb) QueryNodes(id uint64, parent uint64) (res []Node, err error) {
	call := fmt.Sprintf("QueryNodes(%d, %d)", id, parent)
	db.calls = append(db.calls, call)
	err = db.err[call]
	res = db.retNodes[call]
	return
}

// InsertNode stores it's call and returns an error if availiable and a pointer to a node or nil otherwise.
func (db *TestDb) InsertNode(n *Node) (insNode *Node, err error) {
	call := fmt.Sprintf("InsertNode(%#v)", n)
	db.calls = append(db.calls, call)
	err = db.err[call]
	insNode = db.retNode[call]
	return
}

// UpdateNode stores it's call and returns an error if availiable and a pointer to a node or nil otherwise.
func (db *TestDb) UpdateNode(n *Node) (updNode *Node, err error) {
	call := fmt.Sprintf("UpdateNode(%#v)", n)
	db.calls = append(db.calls, call)
	err = db.err[call]
	updNode = db.retNode[call]
	return
}

// DeleteNode stores it's call and returns an error if availiable.
func (db *TestDb) DeleteNode(id uint64) (err error) {
	call := fmt.Sprintf("DeleteNode(%d)", id)
	db.calls = append(db.calls, call)
	err = db.err[call]
	return
}

func TestNewNode(t *testing.T) {
	ds := TestDb{}
	n := NewNode(&ds)
	if n.ds.(*TestDb) != &ds {
		t.Error("NewNode should set datasource in new Node to given address")
	}
}

func TestChildNodes(t *testing.T) {
	ds := TestDb{
		err: map[string]error{
			"QueryNodes(0, 0)": errors.New("Error1"),
		},
		retNodes: map[string][]Node{
			"QueryNodes(0, 2)": []Node{},
			"QueryNodes(0, 3)": []Node{Node{}},
			"QueryNodes(0, 4)": []Node{Node{}, Node{}},
		},
	}
	res, err := ChildNodes(&ds, 0)
	if err.Error() != "Error1" {
		t.Error("ChildNodes(ds, 0) should call QueryNodes(0, 0)")
	}
	if res == nil || len(res) > 0 {
		t.Error("ChildNodes should return empty result but not nil on error.")
	}
	if len(ds.calls) != 1 || ds.calls[0] != "QueryNodes(0, 0)" {
		t.Errorf("ChildNodes should should only call QueryNodes(0, 0), but called %v", ds.calls)
	}
	res, err = ChildNodes(&ds, 1)
	if res == nil || len(res) > 0 {
		t.Error("ChildNodes should return empty slice but not nil, if nil recieved from DataSource")
	}
	res, err = ChildNodes(&ds, 2)
	if err != nil || res == nil || len(res) != 0 {
		t.Error("ChildNodes should return zero results")
	}
	res, err = ChildNodes(&ds, 3)
	if err != nil || res == nil || len(res) != 1 {
		t.Error("ChildNodes should return the one result.")
	}
	res, err = ChildNodes(&ds, 4)
	if err != nil || res == nil || len(res) != 2 {
		t.Error("ChildNodes should return all results.")
	}
	dsSet := true
	for i := range res {
		dsSet = dsSet && res[i].ds.(*TestDb) == &ds
	}
	if !dsSet {
		t.Error("ChildNodes should set DataSource for all results.")
	}
}

func TestRootNodes(t *testing.T) {
	ds := TestDb{
		err: map[string]error{
			"QueryNodes(0, 0)": errors.New("Error1"),
		},
	}
	res, err := RootNodes(&ds) // Should have same effect as ChildNodes(&ds, 0)
	if err.Error() != "Error1" {
		t.Error("ChildNodes(ds, 0) should call QueryNodes(0, 0)")
	}
	if res == nil || len(res) > 0 {
		t.Error("ChildNodes should return empty result but not nil on error.")
	}
	if len(ds.calls) != 1 || ds.calls[0] != "QueryNodes(0, 0)" {
		t.Errorf("ChildNodes should should only call QueryNodes(0, 0), but called %v", ds.calls)
	}
}

func TestIsNew(t *testing.T) {
	n := Node{}
	if !n.IsNew() {
		t.Error("IsNew should be true for nodes with id == 0.")
	}
	n.Id = 10
	if n.IsNew() {
		t.Error("IsNew should be false for nodes with id == 0.")
	}
}
